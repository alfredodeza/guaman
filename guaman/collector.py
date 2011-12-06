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
        self._collect()

    def file_is_valid(self):
        if os.path.isfile(self.path) and self.path.endswith('.csv'):
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

        for root, dirs, files in walk(path):
            for item in files:
                if item.endswith('.csv'):
                    absolute_path = join(root, item)
                    self.append(absolute_path)
