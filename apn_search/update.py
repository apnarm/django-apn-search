import logging

from django.conf import settings

from lazymodel import LazyModel

from apn_search.utils.cache import read_only_cache
from apn_search.utils.indexes import get_index

# TODO: deal with these
# 1. from somewhere import post_commit
#       or consider a middleware approach
#       but that doesn't work from outside of requests
# 2. from mq.backends.sqs_backend import create_backend
#    message_queue = create_backend(region, queue_prefix)
#    maybe leave out the mq requirement and then the user can use this library
#    with any message queue library.

try:
    import cPickle as pickle
except ImportError:
    import pickle


QUEUE_NAME = settings.APN_SEARCH_QUEUE

post_commit_once = post_commit(key=lambda item, **kwargs: LazyModel.get_identifier(item))


@post_commit_once
def update_object(item, remove=False, exception_handling=True):
    """
    Update or remove an object from the search index.

    Accepts an identifier string, a Model instance, or a LazyModel instance.

    Runs after the transaction is committed, allowing for related data
    to be saved before indexing the object.

    """

    try:

        index = get_index(item)

        if isinstance(item, basestring):
            # Dealing with an identifier string.
            if not remove:
                # Create a lazy instance with the read only cache. This means
                # that it can benefit from existing cached objects, but won't
                # fill up the cache with everything that gets indexed here.
                item = LazyModel(item, cache_backend=read_only_cache)

        if not remove and isinstance(item, LazyModel) and not item:
            # The identifier was for an object that does not exist any
            # more, so change this to a remove operation.
            logging.warning('Could not access %r for indexing' % LazyModel.get_identifier(item))
            remove = True

        if remove:

            # Remove this object from the index.
            identifier = LazyModel.get_identifier(item)
            index.remove_object(identifier)

        else:

            # Update this object in the index. This can actually remove the
            # object from the index, if the result of should_index is False.
            index.update_object(item)

    except Exception:
        if exception_handling:
            logging.exception('Error running update_object(%r)' % item, debug_raise=True)
        else:
            raise


@post_commit_once
def queue_update(item, remove=False):
    """Queue an update for the search index."""

    message = {
        'identifier': LazyModel.get_identifier(item)
    }
    if remove:
        message['remove'] = True

    message_body = pickle.dumps(message, -1)

    try:
        with message_queue.open(QUEUE_NAME) as queue:
            queue.put(message_body)
    except Exception:
        logging.exception(
            'Could not send async message. '
            'Running search update immediately.',
            debug_raise=True,
        )
        update_object(item, remove=remove)
