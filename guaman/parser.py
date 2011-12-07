from cStringIO import StringIO
import csv
import hashlib
import re
import sys
import tarfile

from guaman.collector import WantedFiles



class ParseLines(object):

    def __init__(self, fileobject=None):
        self.fileobject = fileobject or StringIO()
        self.csv = csv.reader(self.fileobject)

    def get_timestamp(self, row):
        datere = re.compile('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
        datematch = datere.search(row[0])
        if datematch is None:
            return ''
        return datematch.group(1)


    def get_duration(self, row):
        duration_re  = re.compile('duration: (\d+).(\d+)')
        duration_match = duration_re.search(row[13])

        if duration_match:
            return int(duration_match.group(1))
        return 0


    def create_hash(self, query):
        return hashlib.sha256(query).hexdigest()


    def convert_single_line(self, row):
        statement_re = re.compile('statement: (.*)')

        duration  = self.get_duration(row)
        timestamp = self.get_timestamp(row)
        m = statement_re.search(row[13])

        if m is None:
            query = row[19].lower()
            error = row[13].lower()
        else:
            query = m.group(1).lower()
            error = ''

        if not query:
            return

        return {
                'hash'      : self.create_hash(query),
                'timestamp' : timestamp,
                'user'      : row[1].lower(),
                'db'        : row[2].lower(),
                'open'      : row[7].lower(),
                'status'    : row[11].lower(),
                'ecode'     : row[12].lower(),
                'error'     : error,
                'duration'  : duration,
                'query'     : query,
        }


    def __iter__(self):
        for row in self.csv:
            converted = self.convert_single_line(row)
            if converted:
                yield converted


class dblog(object):
    '''
    takes postgres csv logs from stdin or a file, extract wanted columns of info, stuff
    into a sqlite3 db in memory or file.  Query that db.
    '''
    def __init__(self,file=None,dbfile=':memory:'):

        csv.field_size_limit(1000000000)

        self.file = file
        if self.file == 'stdin':
            self.log = csv.reader(sys.stdin, delimiter=',')
        else:
            self.log =  csv.reader(open(self.file, 'rb'), delimiter=',')
        self.dbfile = dbfile
        self.conn = sqlite3.connect(self.dbfile)
        self.conn.text_factory = str
        self.add_to_sqllite()

    def add_to_sqllite(self):
        p = re.compile('duration: (\d+).(\d+) ms  statement: (.*)')
        datere = re.compile('(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
        c = self.conn.cursor()
        c.execute('''create table logs (tstamp,user,db,op,status,ecode,error,duration,rawquery,query)''')

        for row in self.log:
            datematch = datere.search(row[0])
            if datematch is None:
                tstamp = ''
            else:
                tstamp = datematch.group(1)
     
            user = row[1].lower()
            db = row[2].lower()
            op = row[7].lower()
            status = row[11].lower()
            ecode = row[12].lower()
            m = p.search(row[13])
            if m is None:
                duration = 0
                query = row[19].lower()
                error = row[13].lower()
            else:
                duration = int(m.group(1))
                query = m.group(3).lower()
                error = ''

            rawquery = query
            query = query.replace('"','')
            query = re.sub('\s+',' ',query)
            query = re.sub('_id = \d+','_id = fake',query)
            query = re.sub('limit \d+','limit fake',query)
            query = re.sub('offset \d+','offset fake',query)
            query = re.sub('category_id in .\d+.','category_id in (fake)',query)
            query = re.sub('\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+','faketimestamp',query)

            c.execute('insert into logs values(?,?,?,?,?,?,?,?,?,?)',(tstamp,user,db,op,status,ecode,error,duration,rawquery,query))
        self.conn.commit()
        c.close() 

    def query(self,sql): 
        c = self.conn.cursor()
        c.execute(sql)
        for row in c:
            yield row

if __name__== "__main__":
    import optparse
    parser = optparse.OptionParser('''usage: ???.py -d dbfile -f file -q "query" -s separator''')
    parser.add_option("-f", "--file", dest="file",
        default="stdin", type="string",
        help="input file (stdin by default)") 

    parser.add_option("-q", "--query", dest="query",
        default="", type="string",
        help="query to run") 

    parser.add_option("-s", "--separator", dest="sep",
        default="|", type="string",
        help="separator for output") 

    parser.add_option("-d", "--dbfile", dest="dbfile",
        default=":memory:", type="string",
        help="sqlite3 database file (default is memory)") 

    (options, args) = parser.parse_args()

    log = dblog(file=options.file,dbfile=options.dbfile)

    print options.query
    for row in log.query(options.query):
         print options.sep.join(map(str,row))
    print


    # runs most often
    # select count(*),rawquery from logs where rawquery!='' group by 2 order by 1 desc limit 1;
    # runs most often normalized
    # select count(*),query from logs group by 2 order by 1 desc limit 1;
    # slowest
    # select duration/60000.0,query from logs order by 1 desc limit 1
    # slowest normalized
    # select sum(duration)/60000.0,query from logs group by 2 order by 1 desc limit 1
    # slowest avg query
    # select avg(duration)/60000.0,rawquery from logs where rawquery!='' group by 2 order by 1 desc limit 1;
    # slowest avg query (normalized)
    # select avg(duration)/60000.0,query from logs where op="select" group by 2 order by 1 desc

    # how to get query weight - runs * avg duration?



