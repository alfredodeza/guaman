from mock import patch
import os
from guaman.collector import WantedFiles, Uncompress


describe "path collection":


    before each:
        self.f = WantedFiles(path='/asdf')


    it "should be a list":
        assert isinstance(self.f, list)


    it "does not match files that do not end in csv":
        csv_file = "foo.py"
        assert self.f.valid_csv_file.match(csv_file) == None


    it "matches upper case csv files":
        csv_file = "CASE_foo.csv"
        assert self.f.valid_csv_file.match(csv_file)


    it "does match files without underscores":
        csv_file = "casfoo.csv"
        assert self.f.valid_csv_file.match(csv_file)


    it "matches if it has camelcase":
        csv_file = "CaSe_foo.csv"
        assert self.f.valid_csv_file.match(csv_file)


    it "if it is file and is csv return True":
        with patch('guaman.collector.os.path.isfile'):
            files = WantedFiles('bar.csv')
            assert files.file_is_valid() is True


    it "appends a valid csv file to itself":
        with patch('guaman.collector.os.path.isfile'):
            files = WantedFiles('/bar/foo.csv')
            assert files[0] == '/bar/foo.csv'


