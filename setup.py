import distribute_setup
distribute_setup.use_setuptools()
from setuptools import setup

setup(
    name             = 'guaman',
    description      = 'PostgreSQL query analizer',
    author           = 'Alfredo Deza',
    author_email     = 'alfredodeza [at] gmail.com',
    version          = '0.0.1',
    license          = "MIT",
    scripts          = ['bin/guaman'],
    keywords         = "query, sql, analizer, log",
    install_requires = ['konira>=0.3.0', 'sqlparse'],
    long_description = """

A Log Query analyzer for PostgreSQL
-------------------------------------
Point the command line tool to a single PostgreSQL csv log file or a log 
directory to import the contents and run analytics on the queries.
"""
)
