import json
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from live.availability import Availability
from live.collector import Collector
from live.errors import GenerationChanged, GapOpen, PositionGap, SnapshotRequired
from live.events import EventReader, IncompleteEvent
from live.fixture import serve
from live.source import Channel, EventStream, parse_identifier


class ParsingTests(unittest.TestCase):
    def test_events_may_be_split_at_any_byte(self):
        text = (
            ': keepalive\n\n'
            'id: stream-a:1\nevent: availability\n'
            'data: {\ndata: "kind":"publish",\n'
            'data: "title":"灯火"\ndata: }\n\n'
            'id: stream-a:2\nevent: availability\ndata: {"kind": "withdraw"}\n\n'
        ).encode()
        for size in range(1, 12):
            reader = EventReader()
            events = []
            for offset in range(0, len(text), size):
                events.extend(reader.feed(text[offset:offset + size]))
            events.extend(reader.finish())
            self.assertEqual([event['id'] for event in events], ['stream-a:1', 'stream-a:2'], size)
            self.assertEqual(events[0]['data'], {'kind': 'publish', 'title': '灯火'})

    def test_heartbeats_and_comments_are_not_business_events(self):
        reader = EventReader()
        self.assertEqual(reader.feed(': ping\n\n: ping\n\n'), [])
        self.assertEqual(reader.feed('id: g:3\ndata: {"a":1}\n\n'), [
            {'id': 'g:3', 'data': {'a': 1}}
        ])

    def test_multiline_data_is_one_event(self):
        reader = EventReader()
        events = reader.feed('id: g:1\nevent: availability\ndata: {\ndata: "a": "x\\ny"\ndata: }\n\n')
        self.assertEqual(events, [{'id': 'g:1', 'event': 'availability', 'data': {'a': 'x\ny'}}])

    def test_incomplete_frame_never_becomes_an_event(self):
        reader = EventReader()
        self.assertEqual(reader.feed('id: g:2\ndata: {"a":1}\n\nid: g:3\ndata: {"a"'), [
            {'id': 'g:2', 'data': {'a': 1}}
        ])
        self.assertTrue(reader.pending)
        with self.assertRaises(IncompleteEvent):
            reader.finish()

    def test_multibyte_character_split_across_chunks(self):
        payload = 'id: g:1\ndata: {"title":"归途"}\n\n'.encode()
        cut = payload.index('归'.encode()) + 1
        reader = EventReader()
        self.assertEqual(reader.feed(payload[:cut]), [])
        events = reader.feed(payload[cut:]) + reader.finish()
        self.assertEqual(events[0]['data']['title'], '归途')

    def test_identifier_round_trip(self):
        self.assertEqual(parse_identifier('stream-b:12'), ('stream-b', 12))
        for invalid in ('12', 'stream-a:x', 'stream-a:-1'):
            with self.assertRaises(ValueError):
                parse_identifier(invalid)


class StoreTests(unittest.TestCase):
    def test_snapshot_then_ordered_events(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 0, 'items': []})
        self.assertTrue(book.apply({'generation': 'a', 'position': 1, 'kind': 'publish',
                                    'record': {'id': 'x', 'available': True}}))
        self.assertTrue(book.apply({'generation': 'a', 'position': 2, 'kind': 'withdraw',
                                    'record': {'id': 'x'}}))
        self.assertEqual(book.view(), {'generation': 'a', 'position': 2, 'items': [],
                                       'stale': False, 'missing_from': None})
        self.assertTrue(book.status()['caught_up'])

    def test_hole_is_refused_and_recorded_as_gap(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 0, 'items': []})
        with self.assertRaises(PositionGap):
            book.apply({'generation': 'a', 'position': 2, 'kind': 'publish',
                        'record': {'id': 'x', 'available': True}})
        self.assertEqual(book.boundary(), ('a', 0))
        book.begin_gap('position_gap')
        self.assertEqual(book.gap()['missing_from'], 1)
        with self.assertRaises(PositionGap):
            book.apply({'generation': 'a', 'position': 1, 'kind': 'publish',
                        'record': {'id': 'x', 'available': True}})
        self.assertFalse(book.status()['caught_up'])

    def test_duplicates_are_evident_but_not_applied(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 5, 'items': []})
        self.assertFalse(book.apply({'generation': 'a', 'position': 5, 'kind': 'publish',
                                     'record': {'id': 'x', 'available': True}}))
        self.assertEqual(book.view()['items'], [])
        self.assertEqual(book.status()['discarded'], {'duplicate': 1})
        self.assertIn('duplicate', [row['mechanism'] for row in book.history()])

    def test_new_generation_is_not_compared_by_position(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 9, 'items': []})
        with self.assertRaises(GenerationChanged):
            book.apply({'generation': 'b', 'position': 1, 'kind': 'publish',
                        'record': {'id': 'x', 'available': True}})
        self.assertEqual(book.boundary(), ('a', 9))
        self.assertEqual(book.status()['discarded'], {'superseded': 1})
        book.replace({'generation': 'b', 'position': 0, 'items': []})
        self.assertEqual(book.boundary(), ('b', 0))
        self.assertTrue(book.apply({'generation': 'b', 'position': 1, 'kind': 'publish',
                                    'record': {'id': 'x', 'available': True}}))

    def test_snapshot_cannot_move_back_within_generation(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 3, 'items': []})
        with self.assertRaises(ValueError):
            book.replace({'generation': 'a', 'position': 2, 'items': []})
        book.replace({'generation': 'b', 'position': 0, 'items': []})
        self.assertEqual(book.boundary(), ('b', 0))

    def test_reopen_keeps_view_boundary_and_history(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'availability.db'
            book = Availability(path)
            book.replace({'generation': 'a', 'position': 0, 'items': []})
            book.apply({'generation': 'a', 'position': 1, 'kind': 'publish',
                        'record': {'id': 'x', 'available': True}})
            book.close()
            reopened = Availability(path)
            self.assertEqual(reopened.view(), {
                'generation': 'a', 'position': 1,
                'items': [{'available': True, 'id': 'x'}],
                'stale': False, 'missing_from': None})
            self.assertEqual(len(reopened.coverage()), 2)


class LegacyStoreTests(unittest.TestCase):
    def test_snapshot_only_database_is_migrated(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'legacy.db'
            db = sqlite3.connect(path)
            db.execute('CREATE TABLE state(id INTEGER PRIMARY KEY CHECK(id=1), snapshot TEXT)')
            db.execute('INSERT INTO state VALUES(1, ?)', (json.dumps({
                'generation': 'stream-a', 'position': 5,
                'items': [{'id': 'a', 'available': True}]}),))
            db.commit()
            db.close()
            book = Availability(path)
            self.assertEqual(book.boundary(), ('stream-a', 5))
            self.assertEqual(book.view()['items'], [{'available': True, 'id': 'a'}])
            self.assertTrue(book.apply({
                'generation': 'stream-a', 'position': 6, 'kind': 'publish',
                'record': {'id': 'b', 'available': True}}))
            self.assertEqual(len(book.view()['items']), 2)


class CollectorTests(unittest.TestCase):
    def store(self, directory):
        return Availability(Path(directory) / 'availability.db')

    def test_continuous_follow_captures_short_lived_publish(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            with Collector(url, book, chunk_size=5) as collector:
                collector.sync()
                # Publish and withdraw within the same polling interval.
                source.emit('publish', {'id': 'brief', 'available': True})
                source.emit('withdraw', {'id': 'brief'})
                self.assertTrue(source.wait_for(2))
                self._wait(lambda: book.boundary() == ('stream-a', 2))
                # The brief events are now outside the retained history: a
                # poller resuming at 0 would get 409, while the continuous
                # collector already recorded both positions.
                source.retain_from(2)
                self._wait(lambda: book.boundary() == ('stream-a', 2))
            self.assertEqual({item['id'] for item in book.view()['items']}, {'series-a'})
            self.assertTrue(book.status()['caught_up'])
            self.assertEqual(
                [row['from_position'] for row in book.coverage() if row['mechanism'] == 'stream'],
                [1])

    def test_gap_keeps_last_trusted_view_and_is_not_masked(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'x', 'available': True})
            collector.collect()
            trusted = book.view()
            source.retain_from(10 ** 6)
            source.emit('publish', {'id': 'y', 'available': True})
            source.emit('withdraw', {'id': 'x'})
            with self.assertRaises(SnapshotRequired):
                collector.collect()
            self.assertEqual(book.view()['items'], trusted['items'])
            self.assertTrue(book.view()['stale'])
            self.assertEqual(book.gap(), {
                'generation': 'stream-a', 'covered_through': 1,
                'missing_from': 2, 'reason': 'snapshot_required'})
            self.assertFalse(book.status()['caught_up'])
            self.assertIn('snapshot_required', book.gap()['reason'])

    def test_gap_is_not_cleared_by_new_messages_until_snapshot(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'lost', 'available': True})
            source.retain_from(1)
            with self.assertRaises(SnapshotRequired):
                collector.collect()
            source.emit('publish', {'id': 'later', 'available': True})
            with self.assertRaises(SnapshotRequired):
                collector.collect()
            self.assertFalse(book.status()['caught_up'])
            self.assertEqual(book.boundary(), ('stream-a', 0))
            self.assertEqual(book.view()['items'], [source.snapshot()['items'][0]])
            snapshot, applied = collector.reconcile()
            self.assertTrue(snapshot['position'] >= 2)
            self.assertTrue(book.status()['caught_up'])
            self.assertEqual(book.boundary(), ('stream-a', snapshot['position']))
            self.assertEqual({item['id'] for item in book.view()['items']},
                             {'series-a', 'lost', 'later'})

    def test_events_during_snapshot_handover_are_neither_lost_nor_duplicated(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()

            produced = []

            def during_snapshot(current):
                if produced:
                    return
                produced.extend([
                    current.emit('publish', {'id': 'mid-1', 'available': True}),
                    current.emit('publish', {'id': 'mid-2', 'available': True}),
                    current.emit('withdraw', {'id': 'mid-1'}),
                ])

            source.before_snapshot = during_snapshot
            collector.reconcile()
            source.before_snapshot = None
            self.assertEqual(
                [event['position'] for event in produced], [1, 2, 3])
            self.assertEqual(
                {item['id'] for item in book.view()['items']},
                {'series-a', 'mid-2'})
            self.assertEqual(book.boundary(), ('stream-a', 3))
            # The snapshot covered positions 1..3 and the stream covered the
            # same boundary; no event after 3 is missing.
            self.assertTrue(book.status()['caught_up'])
            coverage = book.coverage()
            self.assertEqual(coverage[-1]['to_position'], 3)

    def test_events_after_snapshot_position_are_delivered_by_the_stream(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()

            def after_snapshot(current):
                if source.after_snapshot is None:
                    return
                source.after_snapshot = None
                current.emit('publish', {'id': 'race-1', 'available': True})
                current.emit('withdraw', {'id': 'race-1'})
                current.emit('publish', {'id': 'race-2', 'available': True})

            source.after_snapshot = after_snapshot
            snapshot, applied = collector.reconcile()
            self.assertEqual(snapshot['position'], 0)
            self.assertEqual(applied, 3)
            self.assertEqual({item['id'] for item in book.view()['items']},
                             {'series-a', 'race-2'})
            self.assertEqual(book.boundary(), ('stream-a', 3))
            self.assertTrue(book.status()['caught_up'])
            # Two snapshots were taken (the explicit sync and the reconcile),
            # then the stream took over from the second one's position.
            self.assertEqual(
                [(row['mechanism'], row['from_position'], row['to_position'])
                 for row in book.coverage()],
                [('snapshot', 0, 0), ('snapshot', 0, 0), ('stream', 1, 3)])

    def test_stale_snapshot_does_not_close_a_gap(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            book.replace({'generation': 'stream-a', 'position': 3,
                          'items': [{'id': 'trusted', 'available': True}]})
            book.begin_gap('snapshot_required')
            collector = Collector(url, book, follow=False, channel=type('Stale', (), {
                'snapshot': lambda self: {'generation': 'stream-a', 'position': 1,
                                          'items': [{'id': 'stale', 'available': True}]},
                'start_events': lambda self, identifier=None, follow=False:
                    (_ for _ in ()).throw(SnapshotRequired('stream-a', 1)),
            })())
            with self.assertRaises(GapOpen):
                collector.reconcile()
            self.assertEqual(book.boundary(), ('stream-a', 3))
            self.assertFalse(book.status()['caught_up'])
            self.assertEqual(book.view()['items'][0]['id'], 'trusted')

    def test_snapshot_at_the_gap_boundary_does_not_close_it(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'a', 'available': True})
            collector.collect()
            # The source refuses the resume and, when asked again, still
            # reports the same snapshot position: it cannot cover the hole.
            book.begin_gap('snapshot_required')
            collector.channel = type('Same', (), {
                'snapshot': lambda self: {'generation': 'stream-a', 'position': 1,
                                          'items': [{'id': 'a', 'available': True}]},
                'start_events': lambda self, identifier=None, follow=False:
                    (_ for _ in ()).throw(SnapshotRequired('stream-a', 1)),
            })()
            with self.assertRaises(GapOpen):
                collector.reconcile()
            self.assertFalse(book.status()['caught_up'])
            self.assertEqual(book.boundary(), ('stream-a', 1))
            self.assertEqual(book.gap()['missing_from'], 2)

    def test_duplicate_replay_is_recorded_not_applied(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'x', 'available': True})
            collector.collect()
            self.assertEqual(book.status()['discarded'], {})
            # A replay at or before the trusted boundary is evident but not
            # applied, even when it is part of a stream run.
            self.assertFalse(book.apply({'generation': 'stream-a', 'position': 1,
                                         'kind': 'publish',
                                         'record': {'id': 'x', 'available': True}}))
            self.assertEqual(book.status()['discarded'], {'duplicate': 1})
            self.assertEqual(len(book.view()['items']), 2)

    def test_truncated_frame_does_not_advance_boundary(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'a', 'available': True})
            source.emit('publish', {'id': 'b', 'available': True})
            body = ': heartbeat\n\n' + self._frame(source, 1) + self._frame(source, 2)
            source.truncate_events_after = body.index('"b"') + 3
            with self.assertRaises(ConnectionError):
                collector.collect()
            self.assertEqual(book.boundary(), ('stream-a', 1))
            source.truncate_events_after = None
            collector.collect()
            self.assertEqual(book.boundary(), ('stream-a', 2))
            self.assertEqual({item['id'] for item in book.view()['items']},
                             {'series-a', 'a', 'b'})

    def test_exit_halfway_through_a_batch_leaves_a_clean_prefix(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            path = Path(directory) / 'availability.db'
            book = Availability(path)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'x', 'available': True})
            source.emit('withdraw', {'id': 'x'})
            source.emit('publish', {'id': 'x', 'available': True, 'title': 'corrected'})
            events = self._events(url)
            # First event applied, then the process "dies" before the rest.
            book.apply(events[0])
            book.close()
            reopened = Availability(path)
            self.assertEqual(reopened.boundary(), ('stream-a', 1))
            self.assertEqual(reopened.coverage()[-1]['to_position'], 1)
            self.assertEqual([item['id'] for item in reopened.view()['items']],
                             ['series-a', 'x'])
            # Restarting from the recorded boundary applies the rest in order.
            collector = Collector(url, reopened, follow=False)
            collector.collect()
            self.assertEqual(reopened.boundary(), ('stream-a', 3))
            titles = [item for item in reopened.view()['items'] if item['id'] == 'x']
            self.assertEqual(titles[0]['title'], 'corrected')

    def test_new_generation_requires_snapshot_and_restarts_positions(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, follow=False)
            collector.sync()
            source.emit('publish', {'id': 'a', 'available': True})
            collector.collect()
            source.new_generation('stream-b')
            source.emit('publish', {'id': 'b', 'available': True})
            with self.assertRaises(SnapshotRequired):
                collector.collect()
            self.assertEqual(book.gap()['reason'], 'generation_changed')
            self.assertFalse(book.status()['caught_up'])
            collector.reconcile()
            self.assertEqual(book.boundary(), ('stream-b', 1))
            self.assertTrue(book.status()['caught_up'])

    def test_source_generation_switch_surfaces_as_snapshot_required(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            source.emit('publish', {'id': 'before', 'available': True})
            collector = Collector(url, book, follow=False)
            collector.sync()
            collector.collect()
            # The source switches generation while the store is catching up.
            source.new_generation('stream-b')
            with self.assertRaises(SnapshotRequired):
                collector.collect()
            self.assertEqual(book.gap()['reason'], 'generation_changed')
            self.assertFalse(book.status()['caught_up'])
            collector.reconcile()
            self.assertEqual(book.boundary()[0], 'stream-b')
            self.assertTrue(book.status()['caught_up'])

    def test_status_can_explain_a_restart(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            path = Path(directory) / 'availability.db'
            collector = Collector(url, Availability(path), follow=False)
            collector.sync()
            source.emit('publish', {'id': 'a', 'available': True})
            collector.collect()
            collector.store.close()
            reopened = Availability(path)
            status = reopened.status()
            self.assertEqual(status['trusted'], {'generation': 'stream-a', 'position': 1})
            self.assertTrue(status['caught_up'])
            self.assertEqual(
                [(row['mechanism'], row['from_position'], row['to_position'])
                 for row in status['coverage']],
                [('snapshot', 0, 0), ('stream', 1, 1)])

    def test_concurrent_thread_keeps_collecting_and_stops(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            book = self.store(directory)
            collector = Collector(url, book, chunk_size=4).start()
            try:
                collector.sync()
                source.emit('publish', {'id': 'one', 'available': True})
                source.emit('withdraw', {'id': 'one'})
                self.assertTrue(source.wait_for(2))
                self._wait(lambda: book.boundary() == ('stream-a', 2))
            finally:
                collector.stop()
            self.assertEqual({item['id'] for item in book.view()['items']}, {'series-a'})

    # ------------------------------------------------------------------
    # helpers

    def _wait(self, predicate, timeout=5):
        import time
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail('condition not reached')

    def _events(self, url):
        connection, response = Channel(url).start_events('stream-a:0')
        try:
            return list(EventStream(response).events())
        finally:
            connection.close()

    def _frame(self, source, position):
        with source.lock:
            event = next(e for e in source.events if e['position'] == position)
        payload = json.dumps(event, ensure_ascii=False, indent=1)
        lines = '\n'.join('data: ' + line for line in payload.splitlines())
        return f'id: {event["generation"]}:{event["position"]}\nevent: availability\n{lines}\n\n'


class SourceBoundaryTests(unittest.TestCase):
    def test_baseline_http_contract_still_holds(self):
        with serve() as (url, source):
            source.emit('publish', {'id': 'b'})
            source.emit('withdraw', {'id': 'b'})
            with urlopen(Request(url + '/events',
                                  headers={'Last-Event-ID': 'stream-a:1'})) as response:
                events = EventReader().read(response.read().decode())
            self.assertEqual([item['id'] for item in events], ['stream-a:2'])
            source.retained_from = 2
            with self.assertRaises(HTTPError) as error:
                urlopen(Request(url + '/events', headers={'Last-Event-ID': 'stream-a:1'}))
            self.assertEqual(error.exception.code, 409)
            self.assertEqual(json.load(error.exception)['error'], 'snapshot_required')

    def test_gap_open_is_reported_by_status(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 2, 'items': []})
        book.begin_gap('snapshot_required')
        with self.assertRaises(GapOpen):
            raise GapOpen('x')
        status = book.status()
        self.assertFalse(status['caught_up'])
        self.assertEqual(status['trusted'], {'generation': 'a', 'position': 2})
        self.assertEqual(status['gap']['missing_from'], 3)
        self.assertEqual(status['coverage'], [
            {'mechanism': 'snapshot', 'generation': 'a', 'from_position': 0,
             'to_position': 2, 'event_count': 0, 'first_sequence': 1,
             'last_sequence': 1}])
