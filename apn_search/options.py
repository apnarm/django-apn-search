import threading

from contextlib import contextmanager

from django.conf import settings


class SearchUpdateOptions(threading.local):
    """
    Temporary search update options for the current thread.

    Setting them:
        with search_update_options(async=False):
            notice.save()

    Accessing them:
        if search_update_options['async']:
            send_message(create_message(notice))

    """

    defaults = {
        'async': True,
        'disabled': False,
        'percolate': True,
    }

    @contextmanager
    def __call__(self, **options):

        before = {}

        for name, value in options.items():

            assert name in self.defaults, 'Unknown index option %r' % name

            if hasattr(self, name):
                before[name] = getattr(self, name)

            setattr(self, name, value)

        yield

        for name, value in options.items():
            if name in before:
                setattr(self, name, before[name])
            else:
                delattr(self, name)

    def __getitem__(self, name):
        overrides = getattr(settings, 'HAYSTACK_UPDATE_OPTIONS', {})
        if name in overrides:
            return overrides[name]
        elif hasattr(self, name):
            return getattr(self, name)
        else:
            return self.defaults[name]


search_update_options = SearchUpdateOptions()
