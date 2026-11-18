"""Command line entry point for the long-running collector.

    python -m live --url http://source --db availability.sqlite3

The process follows the source's event stream and writes each event to the
SQLite store before acknowledging it, prints a status line whenever the
coverage changes, and exits cleanly on SIGINT/SIGTERM.  Re-running it resumes
from the stored boundary; no event is applied twice.
"""

import argparse
import json
import signal
import sys
import time

from .availability import Availability
from .collector import Collector


def main(argv=None):
    parser = argparse.ArgumentParser(prog='python -m live')
    parser.add_argument('--url', required=True, help='source base URL')
    parser.add_argument('--db', required=True, help='SQLite file for the catalogue and history')
    parser.add_argument('--interval', type=float, default=5.0,
                        help='seconds between status lines')
    parser.add_argument('--no-follow', action='store_true',
                        help='poll instead of holding a streaming connection open')
    args = parser.parse_args(argv)

    store = Availability(args.db)
    collector = Collector(args.url, store, follow=not args.no_follow)
    stopping = False

    def request_stop(signum, frame):
        nonlocal stopping
        stopping = True
        collector.stop()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    collector.start()
    last = None
    try:
        while not stopping:
            time.sleep(args.interval)
            status = collector.status()
            line = json.dumps(status, ensure_ascii=False, sort_keys=True)
            if line != last:
                print(line, flush=True)
                last = line
    finally:
        collector.stop()
        store.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
