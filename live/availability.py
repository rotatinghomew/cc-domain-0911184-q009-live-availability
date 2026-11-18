import json
import sqlite3


class Availability:
    def __init__(self, path):
        self.db = sqlite3.connect(path)
        self.db.execute('CREATE TABLE IF NOT EXISTS state(id INTEGER PRIMARY KEY CHECK(id=1), snapshot TEXT)')

    def replace(self, snapshot):
        if not snapshot.get('generation') or snapshot['position'] < 0:
            raise ValueError('invalid snapshot boundary')
        payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True)
        with self.db:
            old = self.db.execute('SELECT snapshot FROM state WHERE id=1').fetchone()
            if old:
                previous = json.loads(old[0])
                if previous['generation'] == snapshot['generation'] and previous['position'] > snapshot['position']:
                    raise ValueError('snapshot moved backwards within generation')
            self.db.execute('INSERT OR REPLACE INTO state VALUES(1,?)', (payload,))

    def current(self):
        row = self.db.execute('SELECT snapshot FROM state WHERE id=1').fetchone()
        return json.loads(row[0]) if row else None
