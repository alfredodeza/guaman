import argparse
import sys

class Commands(object):

    def __init__(self, argv=None, parse=True, test=False):
        self.argv = argv or sys.argv
        if parse:
            self.parse_args(argv)

    def parse_args(self, argv):
        parser = argparse.ArgumentParser(add_help=True, description='A PostgreSQL query analyzer')
        parser.add_argument('--import', action='store', dest='import_path', help='Path to log directory or csv file')
        results = parser.parse_args(argv)

        if results.import_path:
            from guaman.parser import importer
            importer(results.import_path)
            print "All imported"
