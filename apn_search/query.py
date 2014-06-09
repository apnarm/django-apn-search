import json
import logging

from django.conf import settings
from django.contrib.contenttypes.models import ContentType

from haystack import connections, query
from haystack.exceptions import HaystackError

from lazymodel import LazyModel

from apn_search.fields import ForeignKeyField, ManyToManyField
from apn_search.inputs import ModelInput
from apn_search.results import SearchResult
from apn_search.utils.geo import Distance, point_from_lat_long


class DirectSearchQuerySet(query.SearchQuerySet):
    """Allow passing direct instructions to the search backend."""

    def direct(self, **kwargs):
        """Adds "direct" instructions for the search backend."""
        clone = self._clone()
        clone.query.direct(**kwargs)
        return clone

    def filter_wildcard(self, **kwargs):
        """Allows wildcard characters in filters."""
        query = {
            'filter': {
                'query': {
                    'wildcard': kwargs
                }
            }
        }
        return self.direct(**query)


class GeoSearchQuerySet(query.SearchQuerySet):

    def radius_filter(self, min_km=None, max_km=None, order_by=None, **geo_lookup):
        """
        Perform a radius filter from the specified coordinates (lat, long)
        or a Point instance.

        If "order_by" is provided, an extra field is created on each result
        with that name, containing the distance from the coordinates. Results
        are then ordered by the distance. Suggested values to use are
        '-distance' (closest first) or 'distance' (farthest first).

        For example, to search for results within 0 and 5km from the
        coordinates, with the closest results first, you could do:
            SearchQuerySet().radius(0, 5, geoposition=(lng, lat), order_by='distance')

        """

        assert len(geo_lookup) == 1, 'You must supply field_name=coordinates'

        # Ensure the coords are in the right order and recreate the lookup.
        # Always provide coordinates in lat,lng and not the other way around!
        field_name, coordinates = geo_lookup.items()[0]
        point = point_from_lat_long(coordinates)
        geo_lookup = {
            field_name: point.get_coords()
        }

        queryset = self

        if min_km:
            queryset = queryset.dminimum(field_name, point, Distance(km=min_km))

        if max_km:
            queryset = queryset.dwithin(field_name, point, Distance(km=max_km))

        if order_by:
            # Add a distance field to each result.
            assert order_by in ('distance', '-distance')
            queryset = queryset.distance(field_name, point)
            # Sort by that field.
            queryset = queryset.order_by(order_by)

        return queryset


class SearchQuerySet(GeoSearchQuerySet, DirectSearchQuerySet):
    """Extended SearchQuerySet class for general purposes."""

    def __init__(self, *args, **kwargs):
        super(SearchQuerySet, self).__init__(*args, **kwargs)
        self.query.set_result_class(self.create_result)

    def create_result(self, app_label, model_name, pk, score, **kwargs):
        """
        Creates a SearchResult instance, using a custom result class if the
        result's SearchIndex has one defined.

        """

        content_type = ContentType.objects.get_by_natural_key(app_label, model_name)

        unified_index = connections[self.query._using].get_unified_index()
        index = unified_index.get_index(content_type.model_class())

        if hasattr(index, 'get_result_class'):
            result_class = index.get_result_class()
        else:
            result_class = SearchResult

        return result_class(app_label, model_name, pk, score, **kwargs)

    def _convert_model_kwargs(self, **kwargs):
        """
        Converts the values of kwargs into kwargs that are useable with the
        standard queryset methods such as filter and exclude.

        This can also model query lookups, taking advantage of the caching
        system of ModelWithCaching and LazyModel.

        """

        init_args = {}

        for key, value in kwargs.items():

            key = key.split('__', 1)

            field_name = key.pop(0)

            args, kwargs = init_args.setdefault(field_name, ([], {}))

            if key:
                # Handling user__id=123
                # Add the value as a keyword argument,
                # eg. kwargs['id'] = 123
                lookup = key.pop()
                kwargs[lookup] = value
            else:
                # Handling user=User
                # Add the model as an argument
                args.append(value)

        filters = {}

        for field_name, (args, kwargs) in init_args.items():
            filters[field_name] = ModelInput(*args, **kwargs)

        return filters

    def facet(self, field, limit=None):
        queryset = super(SearchQuerySet, self).facet(field)
        if limit:
            facet_fieldname = connections[self.query._using].get_unified_index().get_facet_fieldname(field)
            queryset = queryset.direct(**{
                'facets': {
                    facet_fieldname: {
                        'terms': {
                            'size': limit,
                        }
                    }
                }
            })
        return queryset

    def model_facet_counts(self):
        """
        Get facet counts, same as facet_counts(), but convert any
        ForeignKeyField and ManyToManyField values into LazyModel instances.

        """

        engine = connections[self.query.backend.connection_alias]
        unified_index = engine.get_unified_index()
        fields = unified_index.fields

        facet_counts = self.facet_counts()

        facet_fields = facet_counts.get('fields', {})

        for field_name, values in facet_fields.items():
            field = fields.get(field_name)
            if isinstance(field, (ForeignKeyField, ManyToManyField)):

                new_values = []

                for identifier, count in values:
                    value = LazyModel(identifier)
                    new_values.append((value, count))

                facet_fields[field_name] = new_values

        return facet_counts

    def model_filter(self, **kwargs):
        """
        Performs a filter for a given model and lookup. The benefit of this
        filter is that it does not require you to have the model instance to
        use it.

        Usage:
            SearchQuerySet().model_filter(user=User, user__id=123)
            SearchQuerySet().model_filter(user=User, user__username='bob')

        """
        return self.filter(
            **self._convert_model_kwargs(**kwargs)
        )

    def none(self):
        """Returns an empty result list for the query."""
        return self._clone(klass=EmptySearchQuerySet)

    def order_by(self, *args):
        """
        Alters the order in which the results should appear.

        Totally replaces previous order_by calls,
        which is not what haystack normally does!

        """

        clone = self._clone()

        clone.query.clear_order_by()

        for field in args:
            clone.query.add_order_by(field)

        return clone

    def get_backend_query(self, **kwargs):

        query = self.query

        if query._more_like_this:
            raise NotImplementedError

        search_kwargs = query.build_params()

        if query._raw_query:
            # Special case for raw queries.
            final_query = query._raw_query
            search_kwargs.update(query._raw_query_params)
        else:
            # Normal queries.
            final_query = query.build_query()

        search_kwargs.update(kwargs)

        json_dict = query.backend.build_search_kwargs(final_query, **search_kwargs)

        return json_dict

    def show(self):
        """Show the query that will be sent to the search backend."""
        json_dict = self.get_backend_query()
        print json.dumps(json_dict, indent=4)

    def get_document_ids(self, batch_size=500, verbose=False):
        """Efficiently get the identifier strings of search results."""

        backend = self.query.backend
        backend.setup()

        search_kwargs = self.get_backend_query()

        # Setting fields to an empty array will cause only
        # the _id and _type for each hit to be returned.
        search_kwargs.update({
            'fields': [],
        })

        # Determine which indexes to use for searching, based on the models
        # restriction of the queryset. If none are set, then use all indexes.
        if self.query.models:
            index_names = set()
            for model in self.query.models:
                try:
                    index_names.add(backend.model_index_names[model])
                except KeyError:
                    message = 'No haystack index name found for %r. Mappings are %r' % (
                        model,
                        backend.model_index_names,
                    )
                    logging.warning(message, also_print=settings.DEBUG)
            if not index_names:
                raise HaystackError('No haystack indexes found for %s' % self.query.models)
        else:
            index_names = backend.index_groups.keys()

        start = 0

        while True:

            if verbose:
                print 'Fetching %d to %d from %s' % (
                    start,
                    start + batch_size,
                    ', '.join(index_names),
                )

            query_params = {
                'from': start,
                'size': batch_size,
            }
            results = backend.conn.search(
                None,
                search_kwargs,
                indexes=index_names,
                doc_types=['modelresult'],
                **query_params
            )
            response = results.get('hits', {})
            hits = response.get('hits', [])
            total = response.get('total', 0)

            for hit in hits:
                yield hit['_id']

            start += batch_size
            if start >= total:
                break

    def get_django_ids(self, batch_size=500, verbose=False, strict=False):
        """Efficiently get the database IDs (as strings) of search results."""
        for document_id in self.get_document_ids(batch_size=batch_size, verbose=verbose):
            try:
                app_name, model_name, object_id = document_id.split('.')
            except Exception:
                if strict:
                    raise
                else:
                    logging.warning('Invalid haystack document id: %r' % document_id)
            else:
                yield object_id


class EmptySearchQuerySet(query.EmptySearchQuerySet, SearchQuerySet):
    pass
