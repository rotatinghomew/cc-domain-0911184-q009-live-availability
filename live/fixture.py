import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


class LiveSource:
    def __init__(self):
        self.generation = 'stream-a'
        self.position = 0
        self.items = {'series-a': {'id': 'series-a', 'title': '归途', 'available': True}}
        self.events = []
        self.retained_from = 0
        self.chunk_size = 7
        self.lock = threading.RLock()

    def emit(self, kind, record):
        with self.lock:
            self.position += 1
            event = {'generation': self.generation, 'position': self.position, 'kind': kind, 'record': dict(record)}
            self.events.append(event)
            if kind == 'withdraw':
                self.items.pop(record['id'], None)
            else:
                self.items[record['id']] = dict(record)
            return event

    def snapshot(self):
        with self.lock:
            return {'generation': self.generation, 'position': self.position, 'items': list(self.items.values())}


@contextmanager
def serve(source=None):
    source = source or LiveSource()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlparse(self.path).path
            if path == '/snapshot':
                data = json.dumps(source.snapshot(), ensure_ascii=False).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
            elif path == '/events':
                last = self.headers.get('Last-Event-ID', f'{source.generation}:0')
                generation, separator, position = last.rpartition(':')
                try:
                    position = int(position)
                except ValueError:
                    self.send_error(400)
                    return
                if generation != source.generation or position < source.retained_from:
                    self.send_response(409)
                    self.send_header('Content-Type', 'application/json')
                    data = json.dumps({'error': 'snapshot_required', 'generation': source.generation}).encode()
                else:
                    frames = [': heartbeat\n\n']
                    for event in source.events:
                        if event['position'] > position:
                            payload = json.dumps(event, ensure_ascii=False, indent=1)
                            lines = '\n'.join('data: ' + line for line in payload.splitlines())
                            frames.append(f'id: {source.generation}:{event["position"]}\nevent: availability\n{lines}\n\n')
                    data = ''.join(frames).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
            else:
                self.send_error(404)
                return
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            for offset in range(0, len(data), source.chunk_size):
                self.wfile.write(data[offset:offset + source.chunk_size])
                self.wfile.flush()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', source
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
