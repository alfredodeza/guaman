import re
import os


class WantedFiles(list):
    """
    If a directory is given as input, make sure we aggregate CSV files only
    but look for them all. 
    If there are compressed files and the options tell us to go for them too
    uncompress them properly and lurk for CSV files there too.
    """

    def __init__(self, path):
        self.path = path
        self.compressed_files = []
        self.valid_csv_file = re.compile(r'[_a-zA-Z0-9]+\.csv$', re.IGNORECASE)
        self.valid_compressed_file = re.compile(r'[_a-z-A-Z0-9]+\.(tar|tar.gz|zip|tgz|bz2)$', re.IGNORECASE)
        self._collect()

    def file_is_valid(self):
        endpart = self.path.split('/')[-1]
        if os.path.isfile(self.path) and self.valid_csv_file.match(endpart):
            return True
        return False

    def _collect(self):
        if self.file_is_valid():
            self.append(self.path)
            return

        # Local is faster
        walk = os.walk
        join = os.path.join
        path = self.path
        levels_deep = 0

        for root, dirs, files in walk(path):
            levels_deep += 1

            # Stop digging down after 3 levels down
            if levels_deep > 2:
                continue
            for item in files:
                absolute_path = join(root, item)
                if not self.valid_csv_file.match(item):
                    continue
                self.append(absolute_path)


class Uncompress(list):

    def __init__(self, paths=[]):
        self.paths = paths
        self._get_uncompressed()

    def _get_uncompressed(self):
        pass


