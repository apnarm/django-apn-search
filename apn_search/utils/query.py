from apn_search.inputs import Optional
from apn_search.query import SearchQuerySet
from apn_search.utils.indexes import get_index, get_unified_index


def model_search(*models):
    """Create the basic combined search for the specified models."""

    if not models:
        index = get_unified_index()
        index.build()
        models = index.indexes.keys()

    search = SearchQuerySet().models(*models)

    filters = {}
    for model in models:
        for lookup, value in get_index(model).filters().items():
            # Make the lookup values optional, meaning that it
            # will only apply to documents containing the field.
            filters[lookup] = Optional(value)
    if filters:
        search = search.filter(**filters)

    return search
