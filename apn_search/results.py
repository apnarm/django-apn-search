from django.utils.encoding import smart_str

from haystack import models

from lazymodel import LazyModel


class SearchResult(models.SearchResult):
    """Extended SearchResult class for general purposes."""

    def __getattr__(self, attr):
        """
        The __getattr__ method of Haystack's SearchResult is too lenient.
        This class will raise exceptions if an attribute is missing.

        """

        if attr == '__getnewargs__':
            raise AttributeError

        try:
            return self.__dict__[attr]
        except KeyError:
            raise AttributeError

    def __str__(self):
        return smart_str(unicode(self))

    @property
    def _meta(self):
        return self.model._meta

    @property
    def id(self):
        """Return the database ID instead of the search ID."""
        return self.pk

    @property
    def object(self):
        if self._object is None:
            self._object = LazyModel(self.model, self.pk)
        return self._object

    def get_identifier(self):
        return self.__dict__['id']

    def get_label(self):
        return self.model.get_label()


class LazySearchResult(SearchResult):
    """Get missing attributes from the lazy/cached object."""

    def __unicode__(self):
        return unicode(self.object)

    def __getattr__(self, attr):
        return getattr(self.object, attr)
