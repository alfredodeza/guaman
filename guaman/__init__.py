"""
guaman:

Control options:
    -l, --logging-level     What level of logging is desired, defaults to `info`

Import options:
    -i, --import            Specify a directory or a path to import CSV log files

Reporting options:
    report                  Reporting command
        --limit                 The number of queries to include per report
        all                     Return all the available reports
        weight                  Show only the weighted report
        slowest                 Show only the report with the slowest queries
        usage                   Return the most used queries

    show                    Display all information or a given query, needs the hash
                            as the argument
"""

import sys
import logging

from guaman.parser  import importer
from guaman.argopts import ArgOpts
from guaman.utils   import elephant
from guaman.report  import Report

__version__ = '0.0.1'

class Commands(object):

    def __init__(self, argv=None, parse=True, test=False):
        self.argv = argv or sys.argv
        if parse:
            self.parse_args(self.argv)

    def set_logging(self, level=logging.INFO):
        levels = {
                  'INFO'     : logging.INFO,
                  'DEBUG'    : logging.DEBUG,
                  'WARNING'  : logging.WARNING,
                  'CRITICAL' : logging.CRITICAL
        }

        level = levels.get(level.upper(), logging.INFO)

        logging.basicConfig(
                level  = level,
                format = '%(levelname)s %(message)s',
        )


    def parse_args(self, argv):
        options = [['-i', '--import'], ['-l', '--logging-level'], 'report', 'show', '--limit']
        args    = ArgOpts(options)

        # Help and Version
        args.catch_help    = "%s\n\n %s" % (elephant, __doc__ )
        args.catch_version = "guaman version %s" % __version__
        args.parse_args(argv)

        # Set the logging level
        log_level = args.get('--logging-level', 'INFO')
        self.set_logging(log_level)

        if args.get('--import'):
            importer(args['--import'])
            logging.info('All CSV files successfully imported')

        if args.get('report'):
            report = Report(limit=args.get('--limit'))
            report.generate(args['report'])

        if args.get('show'):
            report = Report()
            report.show(args.get('show'))
