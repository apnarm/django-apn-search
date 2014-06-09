import contextlib
import re
import sys


class ColorContext(object):
    """
    A context manager for terminal text colors.

    Context usage:
        with blue:
            print 'this is blue'
            with red:
                print 'this is red'
            print 'blue again!'

    Callable usage that can break nested colors:
        with purple:
            print 'this is purple'
            print yellow('this is yellow')
            print 'this is not purple!'

    """

    end = '\033[0m'
    stack = [end]

    def __init__(self, start):
        self.start = start

    def __call__(self, text):
        """Colorize some text. Cannot be nested; use as a context instead."""
        return self.start + text + self.end

    def __enter__(self):
        code = self.start
        sys.stdout.write(code)
        sys.stderr.write(code)
        self.stack.append(code)

    def __exit__(self, type, value, traceback):
        self.stack.pop()
        sys.stdout.write(self.stack[-1])
        sys.stderr.write(self.stack[-1])


blue = blue_text = ColorContext('\033[94m')
default = default_text_color = ColorContext(ColorContext.end)
green = green_text = ColorContext('\033[92m')
purple = purple_text = ColorContext('\033[95m')
red = red_text = ColorContext('\033[91m')
yellow = yellow_text = ColorContext('\033[93m')


class FilteredStdOut(object):

    _re_type = type(re.compile(''))

    def __init__(self, stdout, re_pattern):
        self.stdout = stdout
        if not isinstance(re_pattern, self._re_type):
            re_pattern = re.compile(re_pattern)
        self.pattern = re_pattern
        self.blocked = False

    def __getattr__(self, name):
        return getattr(self.stdout, name)

    def write(self, string):
        if self.pattern.search(string):
            self.blocked = True
        elif self.blocked:
            self.blocked = False
            # The print statement writes the newline character afterwards,
            # so this keeps track if what has been filtered out, and then
            # avoids writing whitespace directly afterwards.
            if string.strip():
                self.stdout.write(string)
        else:
            self.stdout.write(string)


@contextlib.contextmanager
def do_not_print(re_pattern):
    """Stop certain messages from being printed to stdout."""
    stdout = sys.stdout
    sys.stdout = FilteredStdOut(stdout, re_pattern)
    try:
        yield
    finally:
        sys.stdout = stdout
