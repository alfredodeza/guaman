import sqlite3

TABLES = "('hash', 'timestamp', 'user', 'db', 'open', 'status', 'ecode', 'error', 'duration', 'query')"
CREATE = {'schema': """CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, hash, timestamp, user, db, open, status, ecode, error, duration, query)""",}


class Database(dict):

    queries = {
            'most_often' : "select hash, query, count(hash) from logs group by hash having count(hash) > 1 order by 3 desc;",
            'total_queries'          : "select count(*) from logs;",
            'slowest'                : "select duration/60000.0,query from logs order by 1 desc limit 1;",
            'slowest_normalized'     : "select hash, query, sum(duration)/60000.0 from logs group by hash order by 3 desc;",
            'slowest_avg'            : "select avg(duration)/60000.0,query from logs where query!='' group by 2 order by 1 desc limit 1;",
            'slowest_avg_normalized' : "select avg(duration)/60000.0,query from logs where open='select' group by 2 order by 1 desc;",
            'weighted'               : "select hash, query, count(hash)*avg(duration+1) from logs group by hash having count(hash) >1 order by 3 desc;"

    }

    cached_queries = {
            'most_often'             : "select hash, query, count from cache_most_often;",
            'slowest_normalized'     : "select hash, query, count from cache_slowest_normalized;",
            'weighted'               : "select hash, query, count from cache_weighted;"
    }

    cached_insert_queries = {
            'most_often'             : "insert into cache_most_often ('hash', 'query', 'count') values (?, ?, ?)",
            'slowest_normalized'     : "insert into cache_slowest_normalized ('hash', 'query', 'count') values (?, ?, ?)",
            'weighted'               : "insert into cache_weighted ('hash', 'query', 'count') values (?, ?, ?)"
    }

    cached_create_tables = {
            'most_often'         : "create table cache_most_often (id integet primary key, hash, query, count)",
            'slowest_normalized' : "create table cache_slowest_normalized (id integet primary key, hash, query, count)",
            'weighted'           : "create table cache_weighted (id integet primary key, hash, query, count)",
    }


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

    def _set_database(self, _type='schema'):
        self.conn.text_factory = str
        if _type == 'schema':
            self.conn.execute(CREATE['schema'])
        else: # we are blowing away the cache
            for table_name in self.cached_queries.keys():
                table_name = 'cache_%s' % table_name
                query = 'DROP TABLE IF EXISTS %s' % table_name
                self.conn.execute(query)
            for key, value in self.cached_create_tables.items():
                self.conn.execute(value)

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

    def populate_cache(self):
        for key in self.cached_queries.keys():
            run = self.c.execute(self.queries[key])
            result = run.fetchall()
            for line in result:
                insert_query = self.cached_insert_queries[key]
                self.c.execute(insert_query, line)
            self.conn.commit()


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

    def __getattr__(self, key):
        #key = self.queries.get(key)
        key = self.cached_queries.get(key)
        if key:
            return self._execute(key)
        raise AttributeError, "DbReport object has no attribute %s" % key

    def _execute(self, query):
        result =  self.c.execute(query)
        return result.fetchall()

    def show(self, _hash):
        _hash = "%s%%" % _hash
        query = "select *, count(hash) from logs where hash like ? group by hash ;"
        result = self.c.execute(query, (_hash,))
        return result.fetchall()

    def cache_is_empty(self):
        # XXX Not used?
        run = self.c.execute('select count(*) from cache_most_often;')
        result = run.fetchone()
        if result.count('*') == 0:
            return True
        return False

