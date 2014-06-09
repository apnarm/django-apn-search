from django.core.cache import cache

from lazycache import LazyCache
from lazycache.lists import CachedList

from apn_search.query import SearchQuerySet


class CachedSearchResults(CachedList):

    def identify_items(self, results):
        for result in results:
            yield result.get_identifier()

    def make_cache_keys(self, identifiers):
        for identifier in identifiers:
            yield 'apn_search.SearchResult:%s' % identifier

    def rebuild_items(self, identifiers):
        return SearchQuerySet().filter(id__in=identifiers)


class ReadOnlyCache(LazyCache):

    def add(self, *args, **kwargs):
        pass

    def decr(self, *args, **kwargs):
        pass

    def incr(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def delete_many(self, *args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        pass

    def set_many(self, *args, **kwargs):
        pass


read_only_cache = ReadOnlyCache(cache)
