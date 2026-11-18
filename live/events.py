"""Incremental parser for the Server-Sent Events stream used by the source.

The source may split the HTTP body at any byte boundary.  A parser instance is
therefore fed one network chunk at a time.  Only a frame terminated by a blank
line produces an event: a partly received frame is kept buffered and never
advances the caller's resume position.
"""

import codecs
import json


class IncompleteEvent(ConnectionError, ValueError):
    """The stream ended in the middle of an event frame.

    It is both a connection failure (retry) and a value error (the document is
    not a complete event file), so callers can handle it either way.  The
    resume position is never advanced for a partially received frame.
    """


class EventReader:
    """Incrementally turn SSE network chunks into business events.

    ``feed`` accepts ``bytes`` (including chunks that end in the middle of a
    UTF-8 character) or ``str``.  It returns the events completed by that
    chunk.  ``finish`` must be called when the response body ends; it raises
    :class:`IncompleteEvent` if a frame was cut off.
    """

    def __init__(self):
        self._decoder = codecs.getincrementaldecoder('utf-8')()
        self._buffer = ''
        self._frame = {}
        self._data = []
        self._at_start = True

    @property
    def pending(self):
        """True when the reader is holding an unfinished line or frame."""

        return bool(self._buffer or self._data or self._frame)

    def feed(self, chunk):
        if isinstance(chunk, str):
            text = chunk
        elif isinstance(chunk, (bytes, bytearray, memoryview)):
            text = self._decoder.decode(bytes(chunk), final=False)
        else:
            raise TypeError('event chunk must be str or bytes')

        if self._at_start and text:
            if text.startswith('﻿'):
                text = text[1:]
            self._at_start = False

        self._buffer += text
        return self._consume(final=False)

    def finish(self):
        """Finish the underlying response body.

        A trailing CR is accepted as a final line terminator.  Any other
        partial line or frame is an error because the cursor must not move
        past an event that was not completely received.
        """

        text = self._decoder.decode(b'', final=True)
        if self._at_start and text:
            if text.startswith('﻿'):
                text = text[1:]
            self._at_start = False
        self._buffer += text

        events = self._consume(final=True)
        if self._buffer or self._data or self._frame:
            raise IncompleteEvent('event stream ended before the event frame was complete')
        return events

    def read(self, text):
        """Read one complete event document.

        This is the strict, one-shot form used for files and tests.  It keeps
        the old behaviour of rejecting a document whose final event is not
        complete.
        """

        events = list(self.feed(text))
        events.extend(self.finish())
        return events

    def _consume(self, final):
        events = []
        buffer = self._buffer
        position = 0

        while position < len(buffer):
            character = buffer[position]
            if character == '\r':
                if position + 1 == len(buffer) and not final:
                    # A CR at the end of the current chunk may be the first
                    # half of CRLF.  Wait for the next chunk.
                    break
                line = buffer[:position]
                if position + 1 < len(buffer) and buffer[position + 1] == '\n':
                    buffer = buffer[position + 2:]
                else:
                    buffer = buffer[position + 1:]
                events.extend(self._consume_line(line))
                position = 0
            elif character == '\n':
                line = buffer[:position]
                buffer = buffer[position + 1:]
                events.extend(self._consume_line(line))
                position = 0
            else:
                position += 1

        self._buffer = buffer
        return events

    def _consume_line(self, line):
        if line == '':
            return self._dispatch()

        if line.startswith(':'):
            # Comment or heartbeat: never a business event.
            return []

        field, separator, value = line.partition(':')
        if not separator:
            value = ''
        elif value.startswith(' '):
            value = value[1:]

        if field == 'data':
            self._data.append(value)
        elif field == 'event':
            self._frame['event'] = value
        elif field == 'id':
            # An id containing NUL is ignored according to the SSE format.
            if '\x00' not in value:
                self._frame['id'] = value
        # "retry" and unknown fields carry no business data.
        return []

    def _dispatch(self):
        data = self._data
        frame = self._frame
        self._data = []
        self._frame = {}

        # Comments and heartbeat frames have no data field and are not
        # business events.
        if not data:
            return []

        identifier = frame.get('id')
        if not identifier:
            raise ValueError('business event requires an id')

        event = {'id': identifier}
        if 'event' in frame:
            event['event'] = frame['event']
        event['data'] = json.loads('\n'.join(data))
        return [event]
