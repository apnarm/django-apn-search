"""
This custom backend checks the responses from ElasticSearch for errors,
which the Haystack and ElasticSearch libraries totally ignore.

It also allows the use of Model instances in queries, automatically
converting them into the format used by ForeignKeyField and ManyToManyField.

"""

import haystack
import logging
import requests

from haystack.backends import elasticsearch_backend, log_query
from haystack.constants import ID, DJANGO_CT
from haystack.exceptions import MissingDependency, HaystackError
from haystack.models import SearchResult
from haystack.utils import get_identifier

try:
    import pyelasticsearch
except ImportError:
    raise MissingDependency("The 'elasticsearch' backend requires the installation of 'pyelasticsearch'. Please refer to the documentation.")

try:
    from django.db.models.sql.query import get_proxied_model
except ImportError:
    # Likely on Django 1.0
    get_proxied_model = None

from django.conf import settings
from django.db.models import Model, Manager
from django.db.models.query import QuerySet

from apn_search.inputs import ModelInput, Optional
from apn_search.utils.dictionaries import merge_dictionaries
from apn_search.utils.indexes import get_index
from apn_search.utils.mappings import find_conflicts


class ElasticSearch(pyelasticsearch.ElasticSearch):

    def _send_request(self, *args, **kwargs):

        response_data = super(ElasticSearch, self)._send_request(*args, **kwargs)

        # It is possible to get a 200 response containing error information.
        # Check for these errors, and raise an exception if any are found.
        items = response_data.get('items')
        if isinstance(items, list):
            errors = []
            for item in items:
                index = item.get('index')
                if isinstance(index, dict):
                    if 'error' in index:
                        errors.append(index['error'])
            if errors:
                raise pyelasticsearch.ElasticSearchError(
                    '%d errors in response: %r' % (len(errors), '\n'.join(errors))
                )

        return response_data

    def from_python(self, value):
        """
        Converts Python values to a form suitable for ElasticSearch's JSON.
        Handle lists of dates!

        """
        if isinstance(value, list):
            return [super(ElasticSearch, self).from_python(item) for item in value]
        else:
            return super(ElasticSearch, self).from_python(value)


class ElasticsearchSearchBackend(elasticsearch_backend.ElasticsearchSearchBackend):

    def __init__(self, connection_alias, **connection_options):
        super(ElasticsearchSearchBackend, self).__init__(connection_alias, **connection_options)
        self.conn = ElasticSearch(connection_options['URL'], timeout=self.timeout)
        self.new_version = bool(connection_options.get('NEW_VERSION'))

    def build_search_kwargs(self, *args, **kwargs):
        direct = kwargs.pop('direct', None)
        search_kwargs = super(ElasticsearchSearchBackend, self).build_search_kwargs(*args, **kwargs)
        return merge_dictionaries(search_kwargs, direct)

    def build_schema(self, fields):

        content_field_name, field_mapping = super(ElasticsearchSearchBackend, self).build_schema(fields)

        if self.new_version:

            for field, mapping in field_mapping.iteritems():

                # Don't use an analyzer for boolean fields.
                # https://github.com/toastdriven/django-haystack/issues/866
                if mapping.get('type') == 'boolean':
                    if mapping.get('index') == 'analyzed':
                        del mapping['index']

                # Newer ES versions handle autocomplete fields differently.
                # http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_index_time_search_as_you_type.html
                if mapping.get('analyzer') in ('ngram_analyzer', 'edgengram_analyzer'):
                    mapping['index_analyzer'] = mapping.pop('analyzer')
                    mapping['search_analyzer'] = 'default'

        return (content_field_name, field_mapping)

    def setup(self):
        """
        Set up the mappings (aka schema) for the index.

        The mappings that are returned from pyelasticsearch are different to
        what Haystack builds, so there is no point in comparing it (which is
        what Haystack normally does).

        Also, it kept breaking when the index did not exist.

        Also, raise an exception and provide extra information if there are
        mapping conflicts, because they are a big deal and ruin everything.

        """

        self.setup_index_groups()

        # Set up each index group individually.
        for index_name, indexes in self.index_groups.items():

            # Make a temporary "unified index" but only for this 1 index.
            isolated_index = ElasticsearchSearchEngine.unified_index()
            isolated_index.build(indexes=indexes)

            # Build the mappings for this index.
            self.content_field_name, field_mapping = self.build_schema(isolated_index.all_searchfields())
            current_mapping = {
                'modelresult': {
                    'properties': field_mapping
                }
            }

            try:

                # Try to push those mappings into ElasticSearch.
                # Make sure the index is there first.
                self.conn.create_index(index_name, self.DEFAULT_SETTINGS)
                self.conn.put_mapping('modelresult', current_mapping, indexes=[index_name])
                self.existing_mapping = current_mapping

            except Exception as error:

                # Something went wrong.
                # Find out what the current mappings are in ElasticSearch.
                try:
                    self.existing_mapping = self.conn.get_mapping()[index_name]
                except KeyError:
                    pass
                except Exception:
                    if not self.silently_fail:
                        raise error

                if settings.DEBUG or settings.TEST_MODE or not self.silently_fail:
                    # Check for obvious conflicts, otherwise just raise the error.
                    try:
                        for field_name in find_conflicts(self.existing_mapping, current_mapping):
                            raise HaystackError("There is a mapping conflict for the %r field. Use the 'check_index' command." % field_name)
                        else:
                            raise error
                    except Exception:
                        raise error
                else:
                    logging.exception(log_function=logging.error)

        self.setup_complete = True

    def setup_index_groups(self):

        if self.setup_complete:
            return

        unified_index = haystack.connections[self.connection_alias].get_unified_index()
        unified_index.setup_indexes()

        index_groups = {}
        index_names = {}
        model_index_names = {}
        for index in unified_index.indexes.values():

            index_name = index.get_index_name(using=self.connection_alias)

            index_groups.setdefault(index_name, []).append(index)
            index_names[index] = index_name
            model_index_names[index.get_model()] = index_name

        self.index_groups = index_groups
        self.index_names = index_names
        self.model_index_names = model_index_names

    def update(self, index, iterable, commit=True):

        if not self.setup_complete:
            try:
                self.setup()
            except pyelasticsearch.ElasticSearchError, e:
                if not self.silently_fail:
                    raise

                self.log.error("Failed to add documents to Elasticsearch: %s", e)
                return

        prepped_docs = []

        for obj in iterable:
            try:
                prepped_data = index.full_prepare(obj)
                final_data = {}

                # Convert the data to make sure it's happy.
                for key, value in prepped_data.items():
                    final_data[key] = self.conn.from_python(value)

                prepped_docs.append(final_data)
            except (requests.RequestException, pyelasticsearch.ElasticSearchError), e:
                if not self.silently_fail:
                    raise

                # We'll log the object identifier but won't include the actual object
                # to avoid the possibility of that generating encoding errors while
                # processing the log message:
                self.log.error(u"%s while preparing object for update" % e.__name__, exc_info=True, extra={
                    "data": {
                        "index": index,
                        "object": get_identifier(obj)
                    }
                })

        index_name = self.index_names[index]

        self.conn.bulk_index(index_name, 'modelresult', prepped_docs, id_field=ID)

        if commit:
            self.conn.refresh(indexes=[index_name])

    def remove(self, obj_or_string, commit=True):
        doc_id = get_identifier(obj_or_string)

        if not self.setup_complete:
            try:
                self.setup()
            except pyelasticsearch.ElasticSearchError, e:
                if not self.silently_fail:
                    raise

                self.log.error("Failed to remove document '%s' from Elasticsearch: %s", doc_id, e)
                return

        index = get_index(doc_id)
        index_name = self.index_names[index]

        try:
            self.conn.delete(index_name, 'modelresult', doc_id)

            if commit:
                self.conn.refresh(indexes=[index_name])
        except (requests.RequestException, pyelasticsearch.ElasticSearchError), e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to remove document '%s' from Elasticsearch: %s", doc_id, e)

    def clear(self, models=[], commit=True):

        if not self.setup_complete:
            # We actually don't want to do the setup here, as mappings could be
            # very different. However, we do need to determine the index groups.
            self.setup_index_groups()

        for index_name in self.index_groups.keys():

            try:

                if not models:
                    self.conn.delete_index(index_name)
                else:

                    models_to_delete = []
                    for model in models:
                        models_to_delete.append("%s:%s.%s" % (DJANGO_CT, model._meta.app_label, model._meta.module_name))

                    # Delete by query in Elasticsearch asssumes you're dealing with
                    # a ``query`` root object. :/
                    self.conn.delete_by_query(index_name, 'modelresult', {'query_string': {'query': " OR ".join(models_to_delete)}})

                if commit:
                    self.conn.refresh(indexes=[index_name])

            except (requests.RequestException, pyelasticsearch.ElasticSearchError), e:
                if not self.silently_fail:
                    raise

                if models:
                    self.log.error("Failed to clear Elasticsearch index of models '%s': %s", ','.join(models_to_delete), e)
                else:
                    self.log.error("Failed to clear Elasticsearch index: %s", e)

    @log_query
    def search(self, query_string, **kwargs):
        if len(query_string) == 0:
            return {
                'results': [],
                'hits': 0,
            }

        if not self.setup_complete:
            self.setup()

        search_kwargs = self.build_search_kwargs(query_string, **kwargs)

        # Because Elasticsearch.
        query_params = {
            'from': kwargs.get('start_offset', 0),
        }

        if kwargs.get('end_offset') is not None and kwargs.get('end_offset') > kwargs.get('start_offset', 0):
            query_params['size'] = kwargs.get('end_offset') - kwargs.get('start_offset', 0)

        # Find the indexes for the specified models,
        # or default to all indexes if no models were supplied.
        models = kwargs.get('models')
        if models:
            index_names = set()
            for model in models:
                try:
                    index_names.add(self.model_index_names[model])
                except KeyError:
                    logging.warning('No haystack index name found for %r. Mappings are %r' % (model, self.model_index_names), also_print=settings.DEBUG)
            if not index_names:
                raise HaystackError('No haystack indexes found for %s' % models)

        else:
            index_names = self.index_groups.keys()

        try:
            raw_results = self.conn.search(None, search_kwargs, indexes=index_names, doc_types=['modelresult'], **query_params)
        except (requests.RequestException, pyelasticsearch.ElasticSearchError), e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to query Elasticsearch using '%s': %s", query_string, e)
            raw_results = {}

        return self._process_results(raw_results, highlight=kwargs.get('highlight'), result_class=kwargs.get('result_class', SearchResult))

    def more_like_this(self, model_instance, additional_query_string=None,
                       start_offset=0, end_offset=None, models=None,
                       limit_to_registered_models=None, result_class=None, **kwargs):
        from haystack import connections

        if not self.setup_complete:
            self.setup()

        # Handle deferred models.
        if get_proxied_model and hasattr(model_instance, '_deferred') and model_instance._deferred:
            model_klass = get_proxied_model(model_instance._meta)
        else:
            model_klass = type(model_instance)

        index = connections[self.connection_alias].get_unified_index().get_index(model_klass)
        field_name = index.get_content_field()
        params = {}

        if start_offset is not None:
            params['search_from'] = start_offset

        if end_offset is not None:
            params['search_size'] = end_offset - start_offset

        doc_id = get_identifier(model_instance)

        # Find the index for the current model. Unfortunately, splitting up the
        # indexes means that MLT results become separated and limited to the
        # that index.
        index_name = self.model_index_names.get(model_klass, self.index_name)

        try:
            raw_results = self.conn.morelikethis(index_name, 'modelresult', doc_id, [field_name], **params)
        except (requests.RequestException, pyelasticsearch.ElasticSearchError), e:
            if not self.silently_fail:
                raise

            self.log.error("Failed to fetch More Like This from Elasticsearch for document '%s': %s", doc_id, e)
            raw_results = {}

        return self._process_results(raw_results, result_class=result_class)


class ElasticsearchSearchQuery(elasticsearch_backend.ElasticsearchSearchQuery):

    def __init__(self, *args, **kwargs):
        super(ElasticsearchSearchQuery, self).__init__(*args, **kwargs)
        self._direct = {}

    def _clone(self, *args, **kwargs):
        clone = super(ElasticsearchSearchQuery, self)._clone(*args, **kwargs)
        clone._direct = self._direct
        return clone

    def build_query_fragment(self, field, filter_type, value):

        optional = isinstance(value, Optional)

        if optional:
            value = value.value

        if isinstance(value, (Model, Manager, QuerySet)):
            value = ModelInput(value)

        fragment = super(ElasticsearchSearchQuery, self).build_query_fragment(field, filter_type, value)

        if optional:
            fragment = '(%s OR (_missing_:%s) OR (NOT (_exists_:%s)))' % (fragment, field, field)

        return fragment

    def direct(self, **kwargs):
        """Adds "direct" instructions for ElasticSearch."""
        self._direct = merge_dictionaries(self._direct, kwargs)

    def build_params(self, *args, **kwargs):
        search_kwargs = super(ElasticsearchSearchQuery, self).build_params(*args, **kwargs)
        search_kwargs['direct'] = self._direct
        return search_kwargs


class ElasticsearchSearchEngine(elasticsearch_backend.ElasticsearchSearchEngine):
    backend = ElasticsearchSearchBackend
    query = ElasticsearchSearchQuery
