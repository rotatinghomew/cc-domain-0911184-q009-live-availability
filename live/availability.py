"""Durable availability store for stream events and snapshot reconciliation.

The database holds one authoritative coverage boundary, the catalogue that
boundary describes, and an audit trail of how the boundary was reached:

* ``generation`` / ``position`` -- the last position the catalogue covers.
  It is written by a full snapshot or by an applied stream event.
* ``gap_*`` -- set when the source refused to resume.  The catalogue is still
  the last trusted one, but ``missing_from`` names the first position that is
  not covered.  While the marker is set, stream events are refused so a
  reconnect that delivers new messages cannot be mistaken for catching up.
* ``history`` -- one row per snapshot boundary, applied event, duplicate or
  superseded message.  This is the source evidence that lets a reader see
  which ranges the stream covered and where a snapshot took over.

Applying an event is a single transaction containing the catalogue change,
the boundary move and the history row.  A process that dies between events
therefore leaves a clean prefix: the catalogue, the boundary and ``history``
agree on exactly which events took effect, and the next position is still
missing.  Duplicates and events from an older generation are recorded too, so
``history`` remains the evidence of what the collector actually received
instead of quietly dropping repeated messages.
"""

import functools
import json
import sqlite3
import threading

from .errors import GenerationChanged, PositionGap


def _now():
    return "strftime('%Y-%m-%dT%H:%M:%fZ','now')"


SCHEMA = """
CREATE TABLE IF NOT EXISTS state(
    id INTEGER PRIMARY KEY CHECK(id=1),
    generation TEXT NOT NULL,
    position INTEGER NOT NULL,
    gap_generation TEXT,
    gap_position INTEGER,
    gap_reason TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS items(
    id TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS history(
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    mechanism TEXT NOT NULL
        CHECK(mechanism IN ('snapshot','stream','duplicate','superseded')),
    generation TEXT NOT NULL,
    from_position INTEGER NOT NULL,
    to_position INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    detail TEXT,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS history_coverage
    ON history(mechanism, generation, from_position, to_position);
"""


def _validate_snapshot(snapshot):
    generation = snapshot.get('generation')
    position = snapshot.get('position')
    if not isinstance(generation, str) or not generation:
        raise ValueError('snapshot generation is required')
    if isinstance(position, bool) or not isinstance(position, int) or position < 0:
        raise ValueError('snapshot position is required')
    items = snapshot.get('items')
    if not isinstance(items, list):
        raise ValueError('snapshot items are required')
    identifiers = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError('snapshot item must be an object')
        if not isinstance(item.get('id'), str) or not item['id']:
            raise ValueError('snapshot item id is required')
        identifiers.append(item['id'])
    if len(set(identifiers)) != len(identifiers):
        raise ValueError('duplicate series in snapshot')
    return generation, position, items


def _validate_event(event):
    generation = event.get('generation')
    position = event.get('position')
    if not isinstance(generation, str) or not generation:
        raise ValueError('event generation is required')
    if isinstance(position, bool) or not isinstance(position, int) or position < 0:
        raise ValueError('event position is required')
    kind = event.get('kind')
    if kind not in ('publish', 'withdraw'):
        raise ValueError(f'unknown event kind: {kind!r}')
    record = event.get('record')
    if not isinstance(record, dict):
        raise ValueError('event record is required')
    if not isinstance(record.get('id'), str) or not record['id']:
        raise ValueError('event record id is required')
    return generation, position, kind, record


def _serialized(method):
    """Serialize access to the shared SQLite connection between threads.

    The collector reads status values from another thread while its
    background loop writes events, so the connection is opened with
    ``check_same_thread=False`` and every public operation takes this lock.
    Transactions are still ``BEGIN IMMEDIATE``, so a second process using the
    same file is serialized by SQLite itself.
    """

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


class Availability:
    """Authoritative local catalogue plus the evidence of how it was reached."""

    def __init__(self, path):
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self._migrate_legacy_schema()
        self.db.executescript(SCHEMA)
        self.db.commit()

    @_serialized
    def close(self):
        self.db.close()

    def _migrate_legacy_schema(self):
        """Carry over a database written by the snapshot-only version.

        That version kept a single JSON blob in ``state(id, snapshot)``.  Its
        boundary is imported as the initial trusted snapshot so an existing
        deployment keeps serving the same catalogue and resumes from there.
        """

        columns = {row[1] for row in self.db.execute('PRAGMA table_info(state)')}
        if not columns or 'snapshot' not in columns or 'generation' in columns:
            return
        row = self.db.execute('SELECT snapshot FROM state WHERE id=1').fetchone()
        self.db.execute('DROP TABLE state')
        self.db.commit()
        if not row:
            return
        legacy = json.loads(row[0])
        _, position, items = _validate_snapshot(legacy)
        self.db.executescript(SCHEMA)
        self.db.execute(
            'INSERT INTO state(id, generation, position) VALUES(1,?,?)',
            (legacy['generation'], position))
        for item in items:
            self.db.execute(
                'INSERT INTO items(id, value) VALUES(?,?)',
                (item['id'], json.dumps(item, ensure_ascii=False, sort_keys=True)))
        self._record('snapshot', legacy['generation'], 0, position, 0,
                     f'imported legacy catalogue ({len(items)} items)')
        self.db.commit()

    # ------------------------------------------------------------------
    # full snapshot

    @_serialized
    def replace(self, snapshot):
        """Atomically replace the catalogue with a full snapshot.

        The snapshot's own position becomes the trusted boundary.  A snapshot
        from a new generation is accepted even when its position is lower than
        the previous generation's: positions are never compared across
        generations.  Within one generation a snapshot that moves backwards is
        rejected, because applying it would drop events already covered.
        """

        generation, position, items = _validate_snapshot(snapshot)

        self.db.execute('BEGIN IMMEDIATE')
        try:
            old = self.db.execute(
                'SELECT generation, position FROM state WHERE id=1').fetchone()
            if old and old[0] == generation and old[1] > position:
                raise ValueError('snapshot moved backwards within generation')
            for item in items:
                self.db.execute(
                    'INSERT INTO items(id, value) VALUES(?,?) '
                    'ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                    (item['id'], json.dumps(item, ensure_ascii=False, sort_keys=True)))
            keep = [item['id'] for item in items]
            if keep:
                marks = ','.join('?' for _ in keep)
                self.db.execute(f'DELETE FROM items WHERE id NOT IN ({marks})', keep)
            else:
                self.db.execute('DELETE FROM items')
            self.db.execute(
                'INSERT INTO state(id, generation, position) VALUES(1,?,?) '
                'ON CONFLICT(id) DO UPDATE SET '
                'generation=excluded.generation, position=excluded.position, '
                'gap_generation=NULL, gap_position=NULL, gap_reason=NULL, '
                f'updated_at={_now()}',
                (generation, position))
            # A snapshot is a complete catalogue, so it covers the whole
            # generation up to its position; event_count stays 0 because no
            # events were applied.  The item count is kept as detail.
            self._record('snapshot', generation, 0, position, 0,
                         f'full catalogue ({len(items)} items)')
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------
    # stream events

    @_serialized
    def apply(self, event):
        """Atomically apply one stream event.

        The event must be exactly the next position of the trusted generation.
        A message at or before the boundary is recorded as a duplicate and is
        *not* applied; a message from another generation is recorded as
        superseded and rejected.  Both cases leave evidence in ``history``
        instead of being silently ignored.
        """

        generation, position, kind, record = _validate_event(event)

        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute(
                'SELECT generation, position, gap_generation, gap_position '
                'FROM state WHERE id=1').fetchone()
            if not row:
                raise ValueError('no snapshot has been applied')
            boundary_generation, boundary_position, gap_generation, gap_position = row
            if gap_generation is not None:
                raise PositionGap(
                    f'gap open from {gap_generation}:{gap_position}; '
                    'reconcile with a snapshot before applying stream events')
            if generation != boundary_generation:
                self._record('superseded', generation, position, position, 1, kind)
                self.db.commit()
                raise GenerationChanged(
                    f'event generation {generation!r} does not match '
                    f'trusted generation {boundary_generation!r}',
                    generation, position)
            if position <= boundary_position:
                self._record('duplicate', generation, position, position, 1, kind)
                self.db.commit()
                return False
            if position != boundary_position + 1:
                raise PositionGap(
                    f'event position {position} does not continue boundary '
                    f'{boundary_generation}:{boundary_position}')

            if kind == 'withdraw':
                self.db.execute('DELETE FROM items WHERE id=?', (record['id'],))
            else:
                self.db.execute(
                    'INSERT INTO items(id, value) VALUES(?,?) '
                    'ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                    (record['id'], json.dumps(record, ensure_ascii=False, sort_keys=True)))
            self.db.execute(
                f'UPDATE state SET position=?, updated_at={_now()} WHERE id=1',
                (position,))
            self._record('stream', generation, position, position, 1, kind)
            self.db.commit()
            return True
        except BaseException:
            if self.db.in_transaction:
                self.db.rollback()
            raise

    # ------------------------------------------------------------------
    # gaps

    @_serialized
    def begin_gap(self, reason='snapshot_required', generation=None, position=None):
        """Keep the last trusted catalogue and record that it is not current.

        By default the gap starts at the trusted boundary.  The catalogue and
        the boundary are untouched.  While the marker is set, :meth:`apply`
        refuses stream events even when new messages arrive, so the store
        cannot report ``caught_up`` before a snapshot fills the gap.
        """

        self.db.execute('BEGIN IMMEDIATE')
        try:
            row = self.db.execute(
                'SELECT generation, position FROM state WHERE id=1').fetchone()
            if not row:
                raise ValueError('no snapshot has been applied')
            generation = row[0] if generation is None else generation
            position = row[1] if position is None else position
            self.db.execute(
                'UPDATE state SET gap_generation=?, gap_position=?, gap_reason=?, '
                f'updated_at={_now()} WHERE id=1',
                (generation, position, reason))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------
    # reads

    @_serialized
    def current(self):
        """The current playable catalogue, or ``None`` before any snapshot."""

        return self.view()

    @_serialized
    def view(self):
        """The playable catalogue as of the trusted boundary.

        ``stale`` is ``True`` while a gap is open: the items are the last
        trusted ones and must not be presented as current.  ``missing_from``
        names the first stream position that is not covered, so a downstream
        reader can see both the data and the hole in one read.
        """

        row = self.db.execute('SELECT generation, position FROM state WHERE id=1').fetchone()
        if not row:
            return None
        items = [json.loads(value) for (value,) in self.db.execute(
            'SELECT value FROM items ORDER BY id')]
        # The playable catalogue: withdrawals delete rows, so only published
        # items are present; this also filters a snapshot's explicit
        # ``available: false`` entries.
        items = [item for item in items if item.get('available', True) is not False]
        gap = self.gap()
        return {
            'generation': row[0],
            'position': row[1],
            'items': items,
            'stale': gap is not None,
            'missing_from': None if gap is None else gap['missing_from'],
        }

    @_serialized
    def boundary(self):
        row = self.db.execute('SELECT generation, position FROM state WHERE id=1').fetchone()
        return None if not row else (row[0], row[1])

    @_serialized
    def cursor(self):
        """The position from which the next stream request may resume.

        An open gap is *not* hidden: the cursor still points at the last
        trusted position, and :meth:`gap` explains that the positions after it
        are not covered.
        """

        return self.boundary()

    @_serialized
    def gap(self):
        row = self.db.execute(
            'SELECT gap_generation, gap_position, gap_reason FROM state WHERE id=1'
        ).fetchone()
        if not row or row[0] is None:
            return None
        return {
            'generation': row[0],
            'covered_through': row[1],
            'missing_from': row[1] + 1,
            'reason': row[2],
        }

    @_serialized
    def status(self):
        """Explain what is covered, what is missing and where each mechanism ran."""

        row = self.db.execute(
            'SELECT generation, position, updated_at FROM state WHERE id=1').fetchone()
        if not row:
            return {
                'trusted': None,
                'gap': None,
                'caught_up': False,
                'coverage': [],
                'discarded': {},
            }
        gap = self.gap()
        counts = {}
        for mechanism, count in self.db.execute(
                "SELECT mechanism, COUNT(*) FROM history "
                "WHERE mechanism IN ('duplicate','superseded') GROUP BY mechanism"):
            counts[mechanism] = count
        return {
            'trusted': {'generation': row[0], 'position': row[1]},
            'gap': gap,
            'caught_up': gap is None,
            'coverage': self.coverage(),
            'discarded': counts,
            'updated_at': row[2],
        }

    @_serialized
    def coverage(self):
        """Contiguous ranges applied by snapshots and by the stream.

        A snapshot segment covers ``0..position`` of its generation because a
        snapshot is a complete catalogue; a stream segment covers the run of
        applied positions.  Consecutive stream events are merged, while
        snapshot entries stay separate so every handover point is visible.
        Each entry carries the history sequence numbers it was built from.
        Duplicate and superseded messages are not merged into these ranges;
        they are reported by :meth:`history`.
        """

        rows = self.db.execute(
            'SELECT sequence, mechanism, generation, from_position, to_position, '
            "event_count FROM history WHERE mechanism IN ('snapshot','stream') "
            'ORDER BY sequence').fetchall()
        segments = []
        for sequence, mechanism, generation, start, end, count in rows:
            last = segments[-1] if segments else None
            if (mechanism == 'stream' and last and last['mechanism'] == 'stream'
                    and last['generation'] == generation
                    and last['to_position'] + 1 == start):
                last['to_position'] = end
                last['event_count'] += count
                last['last_sequence'] = sequence
                continue
            segments.append({
                'mechanism': mechanism,
                'generation': generation,
                'from_position': start,
                'to_position': end,
                'event_count': count,
                'first_sequence': sequence,
                'last_sequence': sequence,
            })
        return segments

    @_serialized
    def history(self):
        rows = self.db.execute(
            'SELECT sequence, mechanism, generation, from_position, to_position, '
            'event_count, detail, recorded_at FROM history ORDER BY sequence').fetchall()
        return [
            {
                'sequence': row[0],
                'mechanism': row[1],
                'generation': row[2],
                'from_position': row[3],
                'to_position': row[4],
                'event_count': row[5],
                'detail': row[6],
                'recorded_at': row[7],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # internals

    def _record(self, mechanism, generation, start, end, count, detail=None):
        self.db.execute(
            'INSERT INTO history(mechanism, generation, from_position, '
            'to_position, event_count, detail) VALUES(?,?,?,?,?,?)',
            (mechanism, generation, start, end, count, detail))
