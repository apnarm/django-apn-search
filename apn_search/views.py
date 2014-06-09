import json

from django.http import HttpResponse, HttpResponseBadRequest

from apn_search.query import EmptySearchQuerySet
from apn_search.utils import regex


class TypeAheadView(object):

    allowed_return_fields = ('pk',)
    default_return_field = 'pk'

    allowed_search_fields = ()
    default_search_field = None

    default_limit = 8

    def __init__(self, base_search, **options):
        self.base_search = base_search
        for name, value in options.items():
            setattr(self, name, value)

    def __call__(self, request):

        # Figure out how to return each result.
        return_field = request.GET.get('return') or self.default_return_field
        if return_field == 'id':
            # Avoid getting "id" because it is the search result ID
            # and not the database ID. The value of "pk" is right.
            return_field = 'pk'
        if return_field not in self.allowed_return_fields:
            return HttpResponseBadRequest('Allowed return values: %s' % ', '.join(self.allowed_return_fields))

        search_field = self.get_search_field(request)

        # Filter the search for the given query.
        query = request.GET.get('query') or request.GET.get('q') or ''
        query = self.clean_query(query)
        if query:

            search = self.build_search(request, search_field, query)

            # Limit the number of results.
            try:
                limit = int(request.GET['limit'])
                assert limit > 0
            except Exception:
                limit = self.default_limit
            search = search[:limit]

        else:
            search = EmptySearchQuerySet()

        # Build the results.
        data = {}
        for result in search:
            name = getattr(result, search_field, None)
            value = getattr(result, return_field, None)
            if name and value:
                data[name] = value

        json_data = json.dumps(data)

        return HttpResponse(json_data, content_type='application/json')

    def build_search(self, request, search_field, query):
        if query:
            return self.base_search.autocomplete(**{search_field: query})
        else:
            return EmptySearchQuerySet()

    def clean_query(self, query):
        """Replace extra whitespace because the search cannot handle it."""
        return regex.whitespace.sub(' ', query).strip()

    def get_search_field(self, request):
        """Figure out which field to search with."""
        search_field = request.GET.get('search-field') or self.default_search_field
        if search_field in self.allowed_search_fields:
            return search_field
        else:
            return HttpResponseBadRequest('Allowed search fields: %s' % ', '.join(self.allowed_search_fields))
