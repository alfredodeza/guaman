import sys

class ArgOpts(dict):


    def __init__(self, options):
        self.options       = options
        self.help          = ['-h', '--h', '--help', 'help']
        self.version       = ['--version', 'version']
        self.catch_help    = None
        self.catch_version = None
        self.exit          = sys.exit
        self.writer        = sys.stdout
        self._arg_count    = {}
        self._count_arg    = {}


    @property
    def flat_options(self):
        flat = []
        for item in self.options:
            if type(item) is list:
                flat.extend(item)
            else:
                flat.append(item)
        return flat


    def _build(self):
        for opt in self.options:
            if type(opt) == list:
                value = self._single_value_from_list(opt)
                if value:
                    for v in opt:
                        self[v] = value
            continue
            value = self._get_value(opt)
            if value:
                self[opt] = self._get_value(opt)
        

    def _single_value_from_list(self, _list):
        for value in _list:
            v = self._get_value(value)
            if v:
                return v


    def parse_args(self, argv):
        self.args = argv[1:]

        # Help and Version:
        self.catches_help()
        self.catches_version()

        self.match = [i for i in argv if i in self.flat_options]
        for count, argument in enumerate(self.args):
            self._arg_count[argument] = count
            self._count_arg[count]    = argument

        # construct the dictionary
        self._build()


    def _get_value(self, opt):
        count = self._arg_count.get(opt)
        if count == None:
            return None
        value = self._count_arg.get(count+1)
        return value


    def has(self, opt):
        if type(opt) == list:
            for i in opt:
                if i in self._arg_count.keys():
                    return True
            return False
        if opt in self._arg_count.keys():
            return True
        return False


    def catches_help(self):
        if self.catch_help:
            print self.catch_help
            print self.args

            if [i for i in self.args if i in self.help]:
                self.writer.write(self.catch_help+'\n')
                self.exit()
            return False


    def catches_version(self):
        if self.catch_version:
            if [i for i in self.args if i in self.version]:
                self.writer.write(self.catch_version+'\n')
                self.exit()
            return False

