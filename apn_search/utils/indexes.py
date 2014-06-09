import inspect

from django.contrib.contenttypes.models import ContentType

from haystack import connections

from lazymodel import LazyModel


def get_backend():
    return connections['default'].get_backend()


def get_unified_index(build=False):
    index = connections['default'].get_unified_index()
    if build:
        index.build()
    return index


def get_index(item):
    """Get the haystack index instance for the provided object."""
    model = _get_model(item)
    return get_unified_index().get_index(model)


def _get_model(item):
    """
    Figure out what model we're dealing with.

    Accepts an identifier string, a Model, a Model instance,
    or a LazyModel instance.

    """

    if isinstance(item, basestring):
        # An identifier string.
        identifier = item

    elif isinstance(item, LazyModel) and not item:
        # A LazyModel instance for an object that does not exist.
        identifier = LazyModel.get_identifier(item)

    else:
        # A valid LazyModel instance or normal Model instance/class.
        # Note: don't use the type() function with LazyModel instances.
        if inspect.isclass(item):
            return item
        else:
            return item.__class__

    # Figure out the model from the identifier string.
    app_label, model, object_pk = identifier.split('.', 2)
    content_type = ContentType.objects.get_by_natural_key(app_label, model)
    return content_type.model_class()
