from collections import namedtuple


DateRangeFacet = namedtuple('DateRangeFacet', ('slug', 'label', 'date_range'))


def get_field_facets(facet_counts, facet_name, key=None):

    def key_method(facet):
        return key(facet[0])

    facets = facet_counts.get('fields', {}).get(facet_name, [])
    facets = [(item, count) for (item, count) in facets if item]
    if key:
        return sorted(facets, key=key_method)

    return facets


def get_date_facets(facet_counts, date_facets):
    facets = []
    query_facets = facet_counts.get('queries', {})
    for key, facet in date_facets.items():
        count = query_facets.get(key, None)
        if count:
            facets.append((facet, count))
    return facets
