import argparse
import sys

class Commands(object):

    def __init__(self, argv=None, parse=True, test=False):
        self.argv = argv or sys.argv
        if parse:
            self.parse_args(argv)

    def parse_args(self, argv):
        parser = argparse.ArgumentParser(description='A PostgreSQL query analyzer')
        parser.parse_args(argv)
