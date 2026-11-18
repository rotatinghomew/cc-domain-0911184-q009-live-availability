import json
import tempfile
from pathlib import Path

from live.availability import Availability
from live.collector import Collector
from live.fixture import serve


def main():
    with tempfile.TemporaryDirectory() as directory, serve() as (url, source):
        available = Availability(Path(directory) / 'availability.sqlite3')
        collector = Collector(url, available, follow=False)

        # Full snapshot first: it establishes the trusted boundary.
        collector.sync()
        print('after snapshot:', json.dumps(available.status(), ensure_ascii=False))

        # A brief publish that is withdrawn again: a polling snapshot would
        # miss it, the continuous stream records both events.
        source.emit('publish', {'id': 'series-b', 'title': '灯火', 'available': True})
        source.emit('withdraw', {'id': 'series-b'})
        collector.collect()
        print('after stream:  ', json.dumps(available.current(), ensure_ascii=False))

        # The source discards its history before the collector asks for it;
        # resuming is refused and the last trusted catalogue is kept,
        # explicitly marked as stale instead of looking caught up.
        source.emit('publish', {'id': 'series-c', 'title': '远山', 'available': True})
        source.retain_from(source.position)
        try:
            collector.collect()
        except Exception as error:
            print('resume refused:', error)
        print('after gap:     ', json.dumps(available.current(), ensure_ascii=False))

        # Reconciliation: a fresh snapshot closes the gap.
        collector.reconcile()
        print('after reconcile:', json.dumps(available.current(), ensure_ascii=False))
        print('coverage:      ', json.dumps(available.coverage(), ensure_ascii=False))


if __name__ == '__main__':
    main()
