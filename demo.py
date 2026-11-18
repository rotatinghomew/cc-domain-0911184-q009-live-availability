import json
import tempfile
from pathlib import Path
from live.availability import Availability
from live.fixture import serve
from live.source import Channel


def main():
    with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
        available = Availability(Path(directory) / 'availability.sqlite3')
        available.replace(Channel(url).snapshot())
        source.emit('publish', {'id': 'series-b', 'title': '灯火', 'available': True})
        available.replace(Channel(url).snapshot())
        print(json.dumps(available.current(), ensure_ascii=False))


if __name__ == '__main__':
    main()
