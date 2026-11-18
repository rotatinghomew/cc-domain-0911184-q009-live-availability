"""Collector: continuous event ingestion with snapshot reconciliation.

The collector never claims that a gap was closed merely because messages
arrived after a reconnect.  :meth:`Collector.status` reports the trusted
boundary, the optional gap and the coverage of each mechanism independently.

Handover during a snapshot: a snapshot names the position it was taken at.
The collector writes that snapshot, then opens the event stream *from the
snapshot's own position*.  Events produced while the snapshot was being
fetched are therefore all delivered after that position: none are lost.  A
replayed event at or before the boundary is recorded as a duplicate and not
applied, so none are applied twice.  Both facts are kept as evidence in the
store's history.

A truncated connection is an ordinary disconnect: every complete frame that
was received stays applied, the cursor does not move past the cut, and the
next reconnect resumes from that cursor.  Only a ``409`` (history gone) or an
event from another generation opens a gap.
"""

import http.client
import sqlite3
import threading

from .availability import Availability
from .errors import GapOpen, GenerationChanged, SnapshotRequired
from .source import Channel, EventStream, format_identifier


class Collector:
    """Follow a resumable event stream and reconcile it with full snapshots.

    ``store`` may be an :class:`~live.availability.Availability` instance or a
    path.  The object is safe to use synchronously (:meth:`run`,
    :meth:`sync`, :meth:`collect`) or as a background thread (:meth:`start`).
    ``follow`` selects a long-lived connection (the default) or one request
    per :meth:`collect` call.
    """

    def __init__(self, url, store, channel=None, chunk_size=4096, follow=True):
        self.url = url
        self.store = store if isinstance(store, Availability) else Availability(store)
        self.channel = channel or Channel(url)
        self.chunk_size = chunk_size
        self.follow = follow
        self._stop = None
        self._thread = None
        self.last_error = None
        self._active = set()
        self._active_lock = threading.Lock()

    # ------------------------------------------------------------------
    # lifecycle

    def start(self):
        """Start the background loop; returns ``self``."""

        if self._thread and self._thread.is_alive():
            return self
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name='live-collector', daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout=10):
        if self._stop is not None:
            self._stop.set()
        # Abort any blocked stream read so the background thread can exit.
        with self._active_lock:
            active = list(self._active)
        for connection, response in active:
            try:
                response.close()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
        if not self._thread:
            return
        self._thread.join(timeout)
        self._thread = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc_info):
        self.stop()

    def status(self):
        """The store's coverage report, including any open gap."""

        return self.store.status()

    # ------------------------------------------------------------------
    # snapshot reconciliation

    def sync(self):
        """Fetch a full snapshot and make it the trusted boundary.

        A snapshot from the same generation must advance past the last trusted
        position (and past an open gap) to be accepted.  Otherwise a
        :class:`GapOpen` is raised and the old trusted catalogue is kept.
        """

        snapshot = self.channel.snapshot()
        gap = self.store.gap()
        if gap is not None and gap['generation'] == snapshot['generation'] \
                and snapshot['position'] <= gap['covered_through']:
            # A snapshot from the same generation that does not advance past
            # the last position we trusted cannot close the gap: it either
            # repeats what we already have or drops information.  Keep the old
            # catalogue and report the problem instead of guessing.
            raise GapOpen(
                f'snapshot at {snapshot["generation"]}:{snapshot["position"]} does not '
                f'reach past the recorded gap at {gap["generation"]}:{gap["covered_through"]}')
        # A snapshot from another generation is authoritative for that
        # generation; positions from the old generation are never compared
        # with it.
        self.store.replace(snapshot)
        return snapshot

    def reconcile(self, attempts=3):
        """Snapshot, then catch up once from the snapshot position.

        If the source has already discarded the events right after the snapshot
        position, another snapshot is fetched: a newer snapshot may cover them.
        After ``attempts`` tries the gap stays recorded (``status()`` keeps
        reporting it) and the last error is raised, because claiming to be
        caught up would hide a real hole.
        """

        last = None
        for _ in range(attempts):
            snapshot = self.sync()
            # The snapshot itself covers everything up to its position, so a
            # resume the source no longer retains is no longer a gap.
            try:
                return snapshot, self.collect(follow=False)
            except SnapshotRequired as error:
                last = error
        raise last

    # ------------------------------------------------------------------
    # event ingestion

    def collect(self, limit=None, follow=None):
        """Apply events until the stream ends, ``limit`` events were applied, or a gap appears.

        A ``409`` records a gap beginning at the trusted boundary but keeps the
        last trusted catalogue.  A truncated frame raises without moving the
        boundary.  Events that belong to a new generation are not compared by
        position; they mark a gap and force a snapshot.  ``follow`` defaults to
        the collector setting; pass ``False`` for a one-shot catch-up.
        """

        follow = self.follow if follow is None else follow
        if self.store.gap() is not None:
            # New messages must not be mistaken for catching up: until a
            # snapshot covers the hole the only correct action is to reconcile.
            raise SnapshotRequired()
        boundary = self.store.boundary()
        try:
            connection, response = self.channel.start_events(
                format_identifier(*boundary) if boundary else None,
                follow=follow)
        except SnapshotRequired as error:
            if boundary is not None and self.store.gap() is None:
                reason = 'snapshot_required'
                if error.generation is not None and error.generation != boundary[0]:
                    # The source named a different generation: positions from
                    # the old one must never be compared with the new ones.
                    reason = 'generation_changed'
                self.store.begin_gap(reason)
            raise

        applied = 0
        self._register(connection, response)
        try:
            stream = EventStream(response, chunk_size=self.chunk_size)
            for event in stream.events(stop=self._stopped):
                try:
                    applied_now = self.store.apply(event)
                except GenerationChanged as error:
                    # A new stream generation restarts its own numbering.  Its
                    # positions must never be compared with the old ones; the
                    # only safe continuation is a fresh snapshot.
                    if self.store.gap() is None:
                        self.store.begin_gap('generation_changed')
                    raise SnapshotRequired(error.generation, error.position) from error
                except ValueError as error:
                    # A hole in the stream or a generation switch cannot be
                    # repaired from the stream itself.  Record the gap before
                    # surfacing the error; the catalogue remains the last
                    # trusted one.
                    if self.store.gap() is None:
                        self.store.begin_gap(
                            'position_gap' if boundary is not None else 'stream_error')
                    raise GapOpen(str(error)) from error
                if applied_now:
                    applied += 1
                if limit is not None and applied >= limit:
                    break
        finally:
            self._unregister(connection, response)
            try:
                response.close()
            finally:
                connection.close()
        return applied

    # ------------------------------------------------------------------
    # background loop

    def run(self, limit=None):
        """One synchronous pass: snapshot if needed, then collect."""

        if self.store.view() is None:
            self.sync()
        return self.collect(limit=limit)

    def _register(self, connection, response):
        with self._active_lock:
            self._active.add((connection, response))

    def _unregister(self, connection, response):
        with self._active_lock:
            self._active.discard((connection, response))

    def _stopped(self):
        return self._stop is not None and self._stop.is_set()

    def _run(self):
        backoff = 0.1
        while not self._stop.is_set():
            try:
                if self.store.view() is None:
                    self.sync()
                self.collect()
                backoff = 0.1
                self.last_error = None
                self._stop.wait(0.05)
            except SnapshotRequired as error:
                # collect() has already marked the gap.  Reconcile, and let the
                # loop resume from the snapshot's boundary.
                self.last_error = error
                try:
                    self.sync()
                except (GapOpen, OSError, ValueError) as retry_error:
                    self.last_error = retry_error
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, 5.0)
                    continue
                backoff = 0.1
            except (GapOpen, OSError, ValueError, sqlite3.Error,
                    http.client.HTTPException) as error:
                # Truncated frames, malformed payloads and transport failures
                # leave the cursor where it was; retry from there.
                self.last_error = error
                self._stop.wait(backoff)
                backoff = min(backoff * 2, 5.0)
