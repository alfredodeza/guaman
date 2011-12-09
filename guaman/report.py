import sys
import sqlparse
from guaman.database import DbReport


class Report(object):

    def __init__(self, limit=None, width=None):
        self.limit = limit or 11
        self.width = width or 50
        self.db = DbReport('/tmp/importer.db')

    def generate(self):
        self.weighted()
        self.slowest_normalized()
        self.most_often()

    def weighted(self):
        weighted = self.db.weighted
        weighted.insert(0, ('Hash', 'Query', 'Weighted Average'))
        self.heading("Weighted Average (execution time * executed times)")

        self.three_column_body(weighted)

    def slowest_normalized(self):
        slowest = self.db.slowest_normalized
        slowest.insert(0, ('Hash', 'Query', 'Seconds'))
        self.heading("Slowest Queries ordered by execution time")

        self.three_column_body(slowest)

    def most_often(self):
        slowest = self.db.most_often
        slowest.insert(0, ('Hash', 'Query', 'Times Executed'))
        self.heading("Most used Queries")

        self.three_column_body(slowest)


    def show(self, _hash):
        show = self.db.show(_hash)
        self.write('')
        for line in show:
            self.write("Hash      ==> %s" % line[1])
            self.write("Timestamp ==> %s" % line[2])
            self.write("User      ==> %s" % line[3])
            self.write("Database  ==> %s" % line[4])
            self.write("Open      ==> %s" % line[5])
            self.write("Status    ==> %s" % line[6])
            self.write("Err Code  ==> %s" % line[7])
            self.write("Error     ==> %s" % line[8])
            self.write("Duration  ==> %s" % line[9])
            self.write("Times ran ==> %s" % line[11])
            self.write("Query     ==> \n\n%s" % sqlparse.format(line[10], reindent=True, keyword_case='upper'))
        self.write('')



    def trim(self, line, width=None):
        width = width or self.width
        return line[:width]


    def heading(self, title):
        self.write('\n'+title+'\n')


    def three_column_body(self, iterator):
        count = 0
        for line in iterator[:self.limit]:
            count += 1
            out_line = "%-8s %-50s - %5s" % (self.trim(line[0], width=8), self.trim(line[1]), line[2])
            self.write(out_line)
            if count == 1:
                self.write('-'*80)

        self.write('')


    def write(self, line):
        sys.stdout.write(line+'\n')
