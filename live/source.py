import json
from urllib.request import urlopen


class Channel:
    def __init__(self, url):
        self.url = url.rstrip('/')

    def snapshot(self):
        with urlopen(self.url + '/snapshot', timeout=5) as response:
            result = json.load(response)
        if not result.get('generation') or not isinstance(result.get('position'), int):
            raise ValueError('snapshot position is required')
        identifiers = [item['id'] for item in result['items']]
        if len(set(identifiers)) != len(identifiers):
            raise ValueError('duplicate series in snapshot')
        return result
