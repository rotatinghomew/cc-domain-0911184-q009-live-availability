import json


class EventReader:
    def read(self, text):
        if not text.endswith('\n\n'):
            raise ValueError('event file must contain complete event frames')
        events = []
        for frame in text.split('\n\n'):
            if not frame.strip():
                continue
            values = {}
            data = []
            for line in frame.splitlines():
                if line.startswith(':'):
                    continue
                field, separator, value = line.partition(':')
                if value.startswith(' '):
                    value = value[1:]
                if field == 'data':
                    data.append(value)
                elif field in ('event', 'id'):
                    values[field] = value
            if not data:
                continue
            if not values.get('id'):
                raise ValueError('business event requires an id')
            values['data'] = json.loads('\n'.join(data))
            events.append(values)
        return events
