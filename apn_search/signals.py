from django.db.models.manager import Manager
from django.db.models.signals import post_save, post_delete

from haystack.utils import get_identifier

from apn_search.options import search_update_options
from apn_search.update import update_object, queue_update


def search_index_signal_handler(instance, signal, **kwargs):
    """
    Signal handler for when indexable objects are saved or deleted.

    The indexing will run after the transaction commits, which will generally
    mean that all of the related ManyToMany data will be saved and ready.

    """

    if search_update_options['disabled']:
        return

    deleting = (signal is post_delete)

    if deleting:
        # When deleting, pass in an identifier string instead of the instance.
        # This is because Django will unset the instance's pk before the update
        # method runs, making it impossible to determine afterwards.
        item = get_identifier(instance)
    else:
        item = instance

    if search_update_options['async']:
        queue_update(item, remove=deleting)
    else:
        update_object(item, remove=deleting)


def make_related_signal_handler(attr_name):
    """
    Create a signal handler function that will get the value of the specified
    attribute of a model instance, and trigger index updates for that value
    or values as though they were changed themselves.

    """

    def related_signal_handler(instance, signal, **kwargs):
        """
        When an object is saved, find related objects which are indexable,
        and trigger the signal handler for them as though they were updated.

        """

        values = getattr(instance, attr_name)
        if callable(values):
            values = values()
        if isinstance(values, Manager):
            values = values.all()
        if not hasattr(values, '__iter__'):
            values = (values,)

        for indexable_item in values:
            # Trigger the post_save signal handler method,
            # as though the indexable object was changed.
            search_index_signal_handler(instance=indexable_item, signal=post_save)

    return related_signal_handler


def related_signal_handler_uid(related_model, indexed_model):
    return 'apn_search.related_signal_handler:%s.%s->%s.%s' % (
        related_model._meta.app_label,
        related_model._meta.module_name,
        indexed_model._meta.app_label,
        indexed_model._meta.module_name,
    )
