import json
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from live.availability import Availability
from live.events import EventReader
from live.fixture import serve
from live.source import Channel


class LiveTests(unittest.TestCase):
    def test_snapshot_fetch_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
            filename = Path(directory) / 'availability.db'
            book = Availability(filename)
            book.replace(Channel(url).snapshot())
            source.emit('publish', {'id': 'series-b', 'title': '新作'})
            book.replace(Channel(url).snapshot())
            self.assertEqual(len(Availability(filename).current()['items']), 2)

    def test_complete_event_file_multiline_and_heartbeat(self):
        text = ': keepalive\n\nid: stream-a:1\nevent: availability\ndata: {\ndata: "kind":"publish"}\n\n'
        events = EventReader().read(text)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['data']['kind'], 'publish')

    def test_partial_event_is_not_a_complete_file(self):
        with self.assertRaises(ValueError):
            EventReader().read('id: stream-a:1\ndata: {')

    def test_stream_resume_and_history_gap(self):
        with serve() as (url, source):
            source.emit('publish', {'id': 'b'})
            source.emit('withdraw', {'id': 'b'})
            with urlopen(Request(url + '/events', headers={'Last-Event-ID': 'stream-a:1'})) as response:
                events = EventReader().read(response.read().decode())
            self.assertEqual([item['id'] for item in events], ['stream-a:2'])
            source.retained_from = 2
            with self.assertRaises(HTTPError) as error:
                urlopen(Request(url + '/events', headers={'Last-Event-ID': 'stream-a:1'}))
            self.assertEqual(error.exception.code, 409)
            self.assertEqual(json.load(error.exception)['error'], 'snapshot_required')

    def test_generation_is_part_of_source_position(self):
        with serve() as (url, source):
            source.generation = 'stream-b'
            with self.assertRaises(HTTPError) as error:
                urlopen(Request(url + '/events', headers={'Last-Event-ID': 'stream-a:100'}))
            self.assertEqual(error.exception.code, 409)

    def test_snapshot_cannot_move_back_within_generation(self):
        book = Availability(':memory:')
        book.replace({'generation': 'a', 'position': 3, 'items': []})
        with self.assertRaises(ValueError):
            book.replace({'generation': 'a', 'position': 2, 'items': []})
        book.replace({'generation': 'b', 'position': 0, 'items': []})
        self.assertEqual(book.current()['generation'], 'b')
