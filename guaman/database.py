import sqlite3

TABLES = "('hash', 'timestamp', 'user', 'db', 'open', 'status', 'ecode', 'error', 'duration', 'query')"
SCHEMA = """CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, hash, timestamp, user, db, open, status, ecode, error, duration, query)"""


class Database(dict):
    
    def __init__(self, path, table='logs'):
        self.table       = table
        self.db_filename = path
        self.conn        = sqlite3.connect(self.db_filename)
        self._set_database()

        # Some default values
        self.select_value  = "SELECT value FROM %s WHERE key= ?" % self.table
        self.insert_values = "INSERT INTO %s %s VALUES (?,?,?,?,?,?,?,?,?,?)" % (self.table, TABLES)
        self.select_all    = "SELECT * from %s" % self.table

    def _set_database(self):
        self.conn.row_factory = sqlite3.Row
        self.conn.text_factory = str
        self.conn.execute(SCHEMA)

    def insert(self, *a):
        self.conn.execute(self.insert_values, a)
        self.conn.commit()

    def _integrity_check(self):
        """Make sure we are doing OK"""
        try:
            integrity = self.conn.execute("pragma integrity_check").fetchone()
            if integrity == (u'ok',):
                return True
        except Exception, error:
            return error

    def _close(self):
        self.conn.close()
