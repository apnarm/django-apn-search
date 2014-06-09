from haystack.inputs import Exact

from django.db.models import Manager
from django.db.models.query import QuerySet

from lazymodel import LazyModel


class ModelInput(Exact):
    """
    An input type for searching on ForeignKeyField and ManyToMany fields.

    Usage:

        user = User.objects.get(pk=123)
        user_ct = ContentType.objects.get_for_model(User)

        SearchQuerySet().filter(user=ModelInput(user))
        SearchQuerySet().filter(user=ModelInput(User, 123))
        SearchQuerySet().filter(user=ModelInput(user_ct, 123))

    """

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], (Manager, QuerySet)):
            value = args[0]
            if isinstance(value, Manager):
                value = value.all()
            model = value.model
            pk_list = value.values_list('pk', flat=True)
            self.query_string = [LazyModel.get_identifier(model, pk) for pk in pk_list]
        else:
            lazy_object = LazyModel(*args, **kwargs)
            if lazy_object.object_pk:
                self.query_string = LazyModel.get_identifier(lazy_object)
            else:
                model = LazyModel.get_model_class(lazy_object)
                raise model.DoesNotExist('%s with lookup %s does not exist.' % (
                    model.__name__, kwargs,
                ))
        self.kwargs = kwargs

    def prepare(self, query_obj):
        return self.query_string


class Optional(object):
    def __init__(self, value):
        if isinstance(value, self.__class__):
            value = value.value
        self.value = value
