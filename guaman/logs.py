import sys


class Logger(object):
    """ A convenient stdout logger """

    def __init__(self, name, debug=False, writer=None):
        self._name = name
        self.writer = writer or sys.stdout
        self.debug = debug

        self.colors = dict(
                            blue   = '\033[94m',
                            green  = '\033[92m',
                            yellow = '\033[93m',
                            red    = '\033[91m',
                            bold   = '\033[1m',
                            ends   = '\033[0m'
        )

    @property
    def name(self):
        if self.debug:
            return "%s " % self._name
        return ''

    def colored_message(self, message='*', color='blue'):
        return "%s%s%s" % (self.colors.get(color, 'blue'), message, self.colors.get('ends'))

    def debug(self, message):
        message = "* %s%s" %  (self.name, message)
        self.writer.write(message)

    def info(self, message):
        message = "%s %s%s" % (self.colored_message(), self.name, message) 
        self.writer.write(message)

    def warning(self, message):
        message = "%s %s%s" % (self.colored_message(color='yellow'), self.name, message) 
        self.writer.write(message)

    def error(self, message):
        message = "%s %s%s" % (self.colored_message(color='red'), self.name, message) 
        self.writer.write(message)

    def critical(self, message):
        message = "%s %s%s" % (self.colored_message(color='red'), self.name, message) 
        self.writer.write(message)

