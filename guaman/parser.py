from cStringIO import StringIO
import csv
import hashlib
import logging
import re

from guaman.collector import WantedFiles
from guaman.database import Database



class ParseLines(object):

    def __init__(self, fileobject=None):
        csv.field_size_limit(1000000000)
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
            logging.debug("row with no query found, skipping")
            return

        return (
                self.create_hash(query), # hash
                timestamp,               # timestamp
                row[1].lower(),          # user
                row[2].lower(),          # db
                row[7].lower(),          # open
                row[11].lower(),         # status
                row[12].lower(),         # ecode
                error,                   # error
                duration,                # duration
                query,                   # query
        )


    def __iter__(self):
        for row in self.csv:
            converted = self.convert_single_line(row)
            if converted:
                yield converted


def importer(path):
    # FIXME: Location of the database has to be a default one
    db = Database('/tmp/importer.db')
    db._set_database(_type='cache') # Blows the cache away
    files = WantedFiles(path)
    count = 0
    for _file in files:
        logging.info("parsing file %s" % _file)
        file_row_count = 0
        for row in ParseLines(open(_file)):
            count += 1
            file_row_count += 1
            db.insert(*row)
        logging.info("Inserted %s rows into database" % file_row_count)
    logging.info("Total rows inserted at import: %s" % count)
    logging.debug("Populating the cache")
    db.populate_cache()
    logging.info("Completed cache creation")
