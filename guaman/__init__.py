import argparse
import sys
import logging

from guaman.parser import importer

class Commands(object):

    def __init__(self, argv=None, parse=True, test=False):
        self.argv = argv or sys.argv
        if parse:
            self.parse_args(argv)

    def set_logging(self, level=logging.INFO):
        levels = {
                'INFO'     : logging.INFO,
                'DEBUG'    : logging.DEBUG,
                'WARNING'  : logging.WARNING,
                'CRITICAL' : logging.CRITICAL
        }
        level = levels.get(level.upper(), logging.INFO)

        logging.basicConfig(
                level    = level,
                format   = '%(levelname)s %(message)s',
        )


    def parse_args(self, argv):
        parser = argparse.ArgumentParser(add_help=True, description='A PostgreSQL query analyzer')
        parser.add_argument('--import', action='store', dest='import_path', help='Path to log directory or csv file')
        parser.add_argument('--logging', action='store', default='INFO', help='Set the verbosity of the script')
        results = parser.parse_args(argv)

        self.set_logging(results.logging)

        if results.import_path:
            importer(results.import_path)
            logging.info('All CSV files successfully imported')
