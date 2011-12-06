from mock import patch
import os
from guaman.collector import WantedFiles


describe "path collection":


    before each:
        self.f = WantedFiles(path='/asdf')


    it "should be a list":
        assert isinstance(self.f, list)


    it "if it is file and is csv return True":
        with patch('guaman.collector.os.path.isfile'):
            files = WantedFiles('bar.csv')
            assert files.file_is_valid() is True


    it "appends a valid csv file to itself":
        with patch('guaman.collector.os.path.isfile'):
            files = WantedFiles('/bar/foo.csv')
            assert files[0] == '/bar/foo.csv'


