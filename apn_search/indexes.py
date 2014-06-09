import logging

from haystack import indexes
from haystack.utils import get_identifier

from django.db.models import signals

from apn_search.query import SearchQuerySet
from apn_search.results import SearchResult
from apn_search.signals import search_index_signal_handler, make_related_signal_handler, related_signal_handler_uid
from apn_search.utils.encoders import BasicEncoder


class CommonSearchIndex(indexes.SearchIndex):

    def _manage_signal_handler(self, signal_method):
        """
        Manage all signal handlers for this index through this method. Provide
        the signal_method (e.g. post_save.connect) and it will do the rest.

        """

        indexed_model = self.get_model()

        # Connect the basic signal handler for this index.
        signal_method(search_index_signal_handler, sender=indexed_model)

        # For any related models, create and connect signal handlers that will
        # update an indexable object when a related object is changed.
        for related_model, field_name in self.get_related_models():

            # Creating the function isn't necessary when disconnecting
            # signals, but for simplicity's sake (and since we never
            # disconnect) just do it anyway.
            related_handler = make_related_signal_handler(field_name)

            unique_id = related_signal_handler_uid(related_model, indexed_model)
            signal_method(
                receiver=related_handler,
                sender=related_model,
                weak=False,
                dispatch_uid=unique_id,
            )

    def _setup_save(self):
        self._manage_signal_handler(signals.post_save.connect)

    def _setup_delete(self):
        self._manage_signal_handler(signals.post_delete.connect)

    def _teardown_save(self):
        self._manage_signal_handler(signals.post_save.disconnect)

    def _teardown_delete(self):
        self._manage_signal_handler(signals.post_delete.disconnect)

    def full_prepare(self, obj):
        super(CommonSearchIndex, self).full_prepare(obj)
        self.prepared_data = BasicEncoder(self.prepared_data).encode()
        return self.prepared_data

    def get_index_name(self, using=None):
        base_index_name = self._get_backend(using).index_name
        model = self.get_model()
        version = self.get_index_version()
        parts = (
            base_index_name,
            model._meta.app_label,
            model._meta.module_name,
            version,
        )
        return '-'.join(str(part) for part in parts if part)

    def get_index_version(self):
        return None

    def get_related_models(self):
        """
        Return a sequence of tuples of models and their field names which
        returns instances of this indexed model. A signal handler will be set
        up to automatically update this index when changes to instances of the
        related model are made.

        """
        return ()

    def get_result_class(self):
        return SearchResult

    def should_index(self, obj):
        """Should this object be indexed?"""
        return True

    def update_object(self, instance, **kwargs):
        """
        Alter the default indexing behaviour to check "should_index" before
        adding an object to the index. If it returns False, then it will be
        removed instead.

        """

        if self.should_index(instance):
            logging.info('Updating search index %r' % get_identifier(instance))
            super(CommonSearchIndex, self).update_object(instance, **kwargs)
            return True
        else:
            self.remove_object(instance, using=None, **kwargs)
            return False

    def remove_object(self, instance, **kwargs):
        logging.info('Removing from search index %r' % get_identifier(instance))
        super(CommonSearchIndex, self).remove_object(instance, **kwargs)

    @staticmethod
    def filters():
        """
        Return a dictionary of filters for searching on this content type.
        These filters should ensure that only live objects are returned.

        Implement this in each Index class.

        """

        return {}

    @classmethod
    def search(cls):
        """
        Perform a basic, default search for this content type.

        Implement this in each Index class.

        """

        return SearchQuerySet()
