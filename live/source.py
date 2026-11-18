"""HTTP client for the source: full snapshots and the resumable event stream."""

import http.client
import json
from urllib.parse import urlsplit

from .errors import SnapshotRequired
from .events import EventReader


def parse_identifier(identifier):
    """Split an SSE ``id`` of the form ``generation:position``."""

    if not isinstance(identifier, str):
        raise ValueError('event id must be a string')
    generation, separator, position = identifier.rpartition(':')
    if not separator or not generation:
        raise ValueError(f'invalid event id: {identifier!r}')
    try:
        position = int(position)
    except ValueError:
        raise ValueError(f'invalid event id position: {identifier!r}') from None
    if position < 0:
        raise ValueError(f'invalid event id position: {identifier!r}')
    return generation, position


def format_identifier(generation, position):
    return f'{generation}:{position}'


class Channel:
    def __init__(self, url, timeout=10):
        self.url = url.rstrip('/')
        self.timeout = timeout

    def _connection(self):
        parts = urlsplit(self.url)
        if parts.scheme not in ('http', 'https'):
            raise ValueError('only http(s) sources are supported')
        if parts.scheme == 'https':
            connection = http.client.HTTPSConnection(
                parts.hostname, parts.port, timeout=self.timeout)
        else:
            connection = http.client.HTTPConnection(
                parts.hostname, parts.port, timeout=self.timeout)
        return parts, connection

    def snapshot(self):
        """Fetch ``GET /snapshot`` and validate its shape."""

        parts, connection = self._connection()
        try:
            connection.request('GET', parts.path + '/snapshot')
            response = connection.getresponse()
            body = response.read()
            if response.status != 200:
                raise OSError(f'snapshot returned HTTP {response.status}')
        finally:
            connection.close()
        result = json.loads(body)
        if not isinstance(result.get('generation'), str) or not result['generation']:
            raise ValueError('snapshot generation is required')
        position = result.get('position')
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise ValueError('snapshot position is required')
        if not isinstance(result.get('items'), list):
            raise ValueError('snapshot items are required')
        identifiers = [item['id'] for item in result['items']]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError('duplicate series in snapshot')
        return result

    def start_events(self, identifier=None, follow=False):
        """Open ``GET /events`` and return ``(connection, response)``.

        The caller owns both objects and must close them.  ``follow`` asks the
        source to keep the response open and push events as they occur.  A
        ``409`` raises :class:`SnapshotRequired`; the error body names the
        current generation and is read before the connection is closed.
        """

        parts, connection = self._connection()
        try:
            headers = {}
            if identifier is not None:
                headers['Last-Event-ID'] = identifier
            path = parts.path + '/events'
            if follow:
                path += '?follow=1'
            connection.request('GET', path, headers=headers)
            response = connection.getresponse()
            if response.status == 409:
                body = response.read()
                try:
                    payload = json.loads(body)
                except ValueError:
                    payload = {}
                raise SnapshotRequired(payload.get('generation'), payload.get('position'))
            if response.status != 200:
                raise OSError(f'event stream returned HTTP {response.status}')
            return connection, response
        except BaseException:
            connection.close()
            raise


class EventStream:
    """Turn one SSE response body into complete business events.

    The response is read with ``read1`` so a long-lived connection is
    consumed as data arrives; the chunk boundaries may fall anywhere and the
    reader keeps partial frames buffered.  Only frames terminated by a blank
    line are yielded, and the reader is finalised at end of body, so a
    connection that dies mid-frame raises
    :class:`~live.events.IncompleteEvent` instead of advancing the cursor.
    """

    def __init__(self, response, reader=None, chunk_size=4096):
        self.response = response
        self.reader = reader or EventReader()
        self.chunk_size = chunk_size

    def raw_events(self, stop=None):
        while True:
            if stop is not None and stop():
                return
            try:
                # ``read1`` returns whatever has arrived instead of blocking
                # until a full buffer is available, which is required for a
                # long-lived stream.  Plain responses fall back to ``read``.
                read = getattr(self.response, 'read1', self.response.read)
                chunk = read(self.chunk_size)
            except (ValueError, AttributeError) as error:
                # The response was closed from another thread (shutdown) while
                # a read was blocked.  Treat it as an ordinary disconnect.
                raise ConnectionError('event stream was closed') from error
            except http.client.IncompleteRead as error:
                # The body declared more bytes than it delivered.  Complete
                # frames parsed before the cut stay valid; a half-received
                # frame must not be applied.  Finalising the reader surfaces
                # that case as IncompleteEvent (also a ConnectionError), and a
                # clean cut is still reported as a connection failure so the
                # caller reconnects from the last applied position.
                if error.partial:
                    yield from self.reader.feed(error.partial)
                # Raises IncompleteEvent when a frame was cut in half.
                self.reader.finish()
                raise
            if not chunk:
                break
            yield from self.reader.feed(chunk)
        yield from self.reader.finish()

    def events(self, stop=None):
        """Yield decoded business event payloads.

        ``stop`` is polled before every read so a follow-mode response can be
        abandoned promptly.  The SSE ``id`` and the JSON payload are
        cross-checked so a mismatched identifier cannot move the cursor.
        """

        for raw in self.raw_events(stop=stop):
            data = raw['data']
            if not isinstance(data, dict):
                raise ValueError('event data must be a JSON object')
            generation, position = parse_identifier(raw['id'])
            if data.get('generation') != generation:
                raise ValueError('event id generation does not match event data')
            if isinstance(data.get('position'), bool) or data.get('position') != position:
                raise ValueError('event id position does not match event data')
            if not isinstance(data.get('record'), dict):
                raise ValueError('event record is required')
            if data.get('kind') not in ('publish', 'withdraw'):
                raise ValueError('unknown event kind')
            yield data
