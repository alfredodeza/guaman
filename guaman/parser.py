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
