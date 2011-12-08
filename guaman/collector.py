import logging
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
        self.valid_endings = ('.csv')
        self._collect()

    def file_is_valid(self):
        if os.path.isfile(self.path) and self.path.endswith(self.valid_endings):
            return True
        return False

    def _collect(self):
        logging.debug("Collecting file(s) in: %s" % self.path)
        if self.file_is_valid():
            logging.info("Using a single path for import: %s" % self.path)
            self.append(self.path)
            return

        # Local is faster
        walk = os.walk
        join = os.path.join
        path = self.path

        for root, dirs, files in walk(path):
            for item in files:
                if item.endswith(self.valid_endings):
                    absolute_path = join(root, item)
                    logging.debug("Collected file path: %s" % absolute_path)
                    self.append(absolute_path)
