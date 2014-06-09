from django.db.models import Model


class Nothing:
    pass


class Encoder(object):
    """
    Encodes objects into another structure.

    This is very similar to a JSONEncoder. The big difference is that
    this will output a dictionary, while a JSONEncoder would output a
    JSON string that represents the dictionary.

    The original purpose of this class it to generate dictionaries that
    can then later on be converted into JSON with very little complexity.

    """

    def __init__(self, obj, **extra):
        self.obj = obj
        self.extra = extra

    @property
    def method_cache(self):
        """Get the method cache for the current class."""
        try:
            return type(self)._method_cache
        except AttributeError:
            result = type(self)._method_cache = {}
            return result

    def methods(self):
        """
        Returns a dictionary mapping of methods and their supported instance
        type(s). Subclasses must implement this.

        """

        raise NotImplementedError

    def encode(self, other_obj=Nothing, **extra):

        if other_obj is not Nothing:
            return self.__class__(other_obj, **extra).encode()

        obj = self.obj
        obj_type = type(obj)

        try:
            method_name = self.method_cache[obj_type]
        except KeyError:

            if not hasattr(self, '_methods'):
                self._methods = self.methods()

            for method, types in self._methods.items():
                if isinstance(obj, types):
                    method_name = method.__name__
                    break
            else:
                if isinstance(obj, dict):
                    method_name = 'dict'
                elif not isinstance(obj, basestring) and hasattr(obj, '__iter__'):
                    method_name = 'list'
                else:
                    method_name = 'default'

            self.method_cache[obj_type] = method_name

        method = getattr(self, method_name)
        return method()

    def default(self):
        return self.obj

    def dict(self):
        return dict(self.dict_items())

    def dict_items(self):
        for key, value in self.obj.iteritems():
            yield key, self.encode(value)

    def list(self):
        return [self.encode(item) for item in self.obj]


class BasicEncoder(Encoder):

    def methods(self):
        return {}

    def default(self):
        if hasattr(self.obj, '__json__'):
            return self.obj.__json__()
        elif isinstance(self.obj, Model):
            return unicode(self.obj)
        else:
            return self.obj
