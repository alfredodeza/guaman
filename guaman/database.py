import sqlite3

TABLES = "('hash', 'timestamp', 'user', 'db', 'open', 'status', 'ecode', 'error', 'duration', 'query')"
SCHEMA = """CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, hash, timestamp, user, db, open, status, ecode, error, duration, query)"""


class Database(dict):

    def __init__(self, path, table='logs'):
        self.table       = table
        self.db_filename = path
        self.conn        = sqlite3.connect(self.db_filename)
        self._set_database()
        self.c           = self.conn.cursor()

        # Some default values
        self.select_value  = "SELECT value FROM %s WHERE key= ?" % self.table
        self.insert_values = "INSERT INTO %s %s VALUES (?,?,?,?,?,?,?,?,?,?)" % (self.table, TABLES)
        self.select_all    = "SELECT * from %s" % self.table

    def _set_database(self):
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



class DbReport(Database):
    """
    Get your reports from queries here. It might be offsetting not to see
    many attributes, or attributes not related to queries since they are
    set on the fly from the `queries` dictionary.

    Available attributes:

    * most_often
    * total_queries
    * slowest
    * slowest_normalized
    * slowest_avg
    * slowest_avg_normalized
    * weighted

    """

    queries = {
            'most_often' : "select hash, query, count(hash) from logs group by hash having count(hash) > 1 order by 3 desc;",
            'total_queries'          : "select count(*) from logs;",
            'slowest'                : "select duration/60000.0,query from logs order by 1 desc limit 1;",
            'slowest_normalized'     : "select hash, query, sum(duration)/60000.0 from logs group by hash order by 3 desc;",
            'slowest_avg'            : "select avg(duration)/60000.0,query from logs where query!='' group by 2 order by 1 desc limit 1;",
            'slowest_avg_normalized' : "select avg(duration)/60000.0,query from logs where open='select' group by 2 order by 1 desc;",
            'weighted'               : "select hash, query, count(hash)*avg(duration+1) from logs group by hash having count(hash) >1 order by 3 desc;"

    }


    def __getattr__(self, key):
        key = self.queries.get(key)
        if key:
            return self._execute(key)
        raise AttributeError, "DbReport object has not attribute %s" % key

    def _execute(self, query):
        result =  self.c.execute(query)
        return result.fetchall()

    def show(self, _hash):
        _hash = "%s%%" % _hash
        query = "select *, count(hash) from logs where hash like ? group by hash ;"
        result = self.c.execute(query, (_hash,))
        return result.fetchall()
