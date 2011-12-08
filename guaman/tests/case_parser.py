import hashlib
from guaman.parser import ParseLines


describe "parsing csv lines":

    before each:
        self.parser = ParseLines()
        self.csv_rows = ['' for i in range(30)]

    it "returns none for empty queries":
        result = self.parser.convert_single_line(self.csv_rows)
        assert result is None

    it "returns a dict with my query if found":
        self.csv_rows[13] = "duration: 15.0 ms  statement: SELECT * FROM foo"
        result = self.parser.convert_single_line(self.csv_rows)
        query = 'select * from foo'
        assert result != None
        assert result[0] == hashlib.sha256(query).hexdigest()
        assert result[1] == ''
        assert result[2] == ''
        assert result[3] == ''
        assert result[4] == ''
        assert result[5] == ''
        assert result[6] == ''
        assert result[7] == ''
        assert result[8] == 15
        assert result[9] == query

    it "returns the date if it matches the regex":
        self.csv_rows[0] = '1999-22-01 11:11:11'
        result = self.parser.get_timestamp(self.csv_rows)
        assert result == '1999-22-01 11:11:11'

    it "returns an empty string it the regex does not match":
        result = self.parser.get_timestamp(self.csv_rows)
        assert result == ''

    it "deals with statement and no durations":
        self.csv_rows[13] = 'statement: SELECT * FROM bar'
        result = self.parser.convert_single_line(self.csv_rows)
        assert result != None
        assert result[-1] == 'select * from bar'
        assert result[-2] == 0

    it "converts float durations to int":
        self.csv_rows[13] = "duration: 15.44 ms"
        result = self.parser.get_duration(self.csv_rows)
        assert result == 15

    it "returns zero when it cannot find a duration":
        result = self.parser.get_duration(self.csv_rows)
        assert result == 0
