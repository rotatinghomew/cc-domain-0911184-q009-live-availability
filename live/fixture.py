"""Local source used by the tests and the demo.

The HTTP surface mirrors the real source:

* ``GET /snapshot`` returns ``generation``, ``position`` and ``items``.
* ``GET /events`` reads ``Last-Event-ID: generation:position`` and returns
  ``409 snapshot_required`` when the position is no longer retained or belongs
  to another generation.
* events are SSE frames; payloads are multi-line JSON, comments are used as
  heartbeats, and the body is written in chunks of ``chunk_size`` bytes so the
  parser is exercised across arbitrary network boundaries.

By default a request returns the currently retained events and closes the
body, which keeps the baseline behaviour.  ``?follow=1`` keeps the response
open and pushes events as they are emitted, which is what the long-running
collector uses.  ``truncate_events_after`` lets a test cut a body in the middle
of a frame, and ``retain_from`` drops the history so a resume is refused.
"""

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class LiveSource:
    def __init__(self):
        self.generation = 'stream-a'
        self.position = 0
        self.items = {'series-a': {'id': 'series-a', 'title': '归途', 'available': True}}
        self.events = []
        self.retained_from = 0
        self.chunk_size = 7
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        # Test hooks.  ``before_snapshot`` runs before the snapshot body is
        # captured (its events are part of the snapshot); ``after_snapshot``
        # runs after the snapshot position was captured but before the body is
        # written (its events are *not* in the snapshot and must be delivered
        # by the stream, which is the real handover race).
        self.before_snapshot = None
        self.after_snapshot = None
        # When set, an /events body is cut off after this many bytes, which
        # simulates a connection that dies in the middle of a frame.
        self.truncate_events_after = None

    # ------------------------------------------------------------------
    # source model

    def emit(self, kind, record):
        with self.condition:
            self.position += 1
            event = {
                'generation': self.generation,
                'position': self.position,
                'kind': kind,
                'record': dict(record),
            }
            if kind == 'withdraw':
                self.items.pop(record['id'], None)
            else:
                self.items[record['id']] = dict(record)
            self.events.append(event)
            self.condition.notify_all()
            return event

    def snapshot(self):
        with self.lock:
            return {
                'generation': self.generation,
                'position': self.position,
                'items': [dict(item) for item in self.items.values()],
            }

    def retain_from(self, position):
        """Drop history before ``position``; resuming earlier answers 409."""

        with self.lock:
            self.retained_from = position
            self.events = [event for event in self.events if event['position'] > position]

    def new_generation(self, generation):
        """Start a new stream generation whose positions start again at 0."""

        with self.condition:
            self.generation = generation
            self.position = 0
            self.events = []
            self.retained_from = 0
            self.condition.notify_all()
            return generation

    def emitted_after(self, generation, position):
        with self.lock:
            return [
                event for event in self.events
                if event['generation'] == generation and event['position'] > position
            ]

    def wait_for(self, position, generation=None, timeout=5):
        """Wait until the source has emitted ``position`` in ``generation``."""

        with self.condition:
            return self.condition.wait_for(
                lambda: self.generation == (generation or self.generation)
                and self.position >= position, timeout)


def _frame(event):
    payload = json.dumps(event, ensure_ascii=False, indent=1)
    lines = '\n'.join('data: ' + line for line in payload.splitlines())
    return f'id: {event["generation"]}:{event["position"]}\nevent: availability\n{lines}\n\n'


@contextmanager
def serve(source=None):
    source = source or LiveSource()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            follow = query.get('follow', ['0'])[0] not in ('0', 'false', '')

            if path == '/snapshot':
                hook = source.before_snapshot
                if hook is not None:
                    hook(source)
                data = json.dumps(source.snapshot(), ensure_ascii=False).encode()
                hook = source.after_snapshot
                if hook is not None:
                    hook(source)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self._write_all(data)
                return

            if path != '/events':
                self.send_error(404)
                return

            last = self.headers.get('Last-Event-ID', f'{source.generation}:0')
            generation, separator, position = last.rpartition(':')
            try:
                position = int(position)
            except ValueError:
                self.send_error(400)
                return
            if not separator:
                self.send_error(400)
                return
            with source.lock:
                current_generation = source.generation
                retained_from = source.retained_from
            if generation != current_generation or position < retained_from:
                data = json.dumps(
                    {'error': 'snapshot_required', 'generation': current_generation,
                     'position': position},
                    ensure_ascii=False).encode()
                self.send_response(409)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self._write_all(data)
                return

            if follow:
                self._follow(generation, position)
            else:
                data = self._body(generation, position)
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                limit = source.truncate_events_after
                if limit is not None:
                    self._write_all(data[:limit])
                    self.close_connection = True
                else:
                    self._write_all(data)
            return

        def _body(self, generation, position):
            with source.lock:
                events = [
                    event for event in source.events
                    if event['generation'] == generation and event['position'] > position
                ]
            frames = [': heartbeat\n\n']
            frames.extend(_frame(event) for event in events)
            return ''.join(frames).encode()

        def _follow(self, generation, position):
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            if not self._chunk(b': heartbeat\n\n'):
                return
            last = position
            while True:
                with source.condition:
                    source.condition.wait_for(
                        lambda: source.generation != generation
                        or any(event['position'] > last for event in source.events),
                        timeout=0.05)
                    if source.generation != generation:
                        # A generation switch ends the connection cleanly: its
                        # positions cannot be compared with the new ones, and
                        # the next resume will be answered with 409.
                        self._end()
                        return
                    events = [
                        event for event in source.events
                        if event['generation'] == generation and event['position'] > last
                    ]
                if not events:
                    if not self._chunk(b': heartbeat\n\n'):
                        return
                    continue
                for event in events:
                    frame = _frame(event).encode()
                    # Write the frame in small pieces so a reader sees it split
                    # across arbitrary network boundaries.
                    for offset in range(0, len(frame), source.chunk_size):
                        if not self._chunk(frame[offset:offset + source.chunk_size]):
                            return
                    last = event['position']

        def _end(self):
            try:
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass

        def _chunk(self, data):
            try:
                self.wfile.write(f'{len(data):X}\r\n'.encode())
                self.wfile.write(data)
                self.wfile.write(b'\r\n')
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return False
            return True

        def _write_all(self, data):
            for offset in range(0, len(data), source.chunk_size):
                try:
                    self.wfile.write(data[offset:offset + source.chunk_size])
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    return


        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', source
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
