from django.core.files.storage import default_storage
from django.db.models import Model
from django.db.models.query import QuerySet
from django.db.models.manager import Manager
from django.template import Context, Template

from haystack.fields import CharField, DateTimeField, FacetField, MultiValueField, SearchField

from lazymodel import LazyModel

from apn_search.utils.geo import point_from_lat_long


class TemplateField(CharField):
    """A shortcut for creating a template field using a string."""

    def __init__(self, template, **kwargs):

        # Strip off excess whitespace from the template and compile it.
        template_lines = []
        for line in template.splitlines():
            line = line.strip()
            if line:
                template_lines.append(line)
        template = '\n'.join(template_lines)
        self.compiled_template = Template(template)

        kwargs['use_template'] = True

        super(TemplateField, self).__init__(**kwargs)

    def prepare_template(self, obj):
        return self.compiled_template.render(Context({'object': obj}))


class DocumentTemplateField(TemplateField):
    """
    A standard way to define a document template for a model.

    Use this once per Index class. Always use the same field name
    when using this. We are using "text" in this codebase.

    """

    def __init__(self, *args, **kwargs):
        kwargs['document'] = True
        super(DocumentTemplateField, self).__init__(*args, **kwargs)


class ForeignKeyField(SearchField):
    """A field that lets you access a related database object."""

    field_type = 'string'

    def prepare(self, obj):
        value = super(ForeignKeyField, self).prepare(obj)
        if value:
            return LazyModel.get_identifier(value)

    def convert(self, value):
        """Returns a lazy object which fetches the related object."""
        if value:
            return LazyModel(value)


class ManyToManyQuerySet(QuerySet):
    """
    A minimal QuerySet for ManyToManyField results.
    Implements only some of the usual Model QuerySet methods.

    """

    def __init__(self, manager):
        self._iter = False
        self._manager = manager
        self._result_cache = manager._values

    def _clone(self):
        return self

    def iterator(self):
        for item in self._result_cache:
            yield item

    @property
    def model(self):
        return self._manager.model


class ManyToManyManager(Manager):
    """
    A minimal Manager for ManyToManyField results.
    Implements only some of the usual Model Manager methods.
    This will break if you try anything fancy.

    """

    def __init__(self, *values):
        self._values = values

    def get_query_set(self):
        return ManyToManyQuerySet(self)

    @property
    def model(self):
        if not hasattr(self, '_model'):
            for value in self._values:
                if value:
                    self._model = value.__class__
                    break
            else:
                self._model = None
        return self._model


class ManyToManyField(MultiValueField):

    def prepare(self, obj):

        value = super(MultiValueField, self).prepare(obj)

        if value is None:
            return None

        if isinstance(value, Manager):
            value = value.all()

        if isinstance(value, QuerySet):
            model = value.model
            pk_list = value.values_list('pk', flat=True)
            return [LazyModel.get_identifier(model, pk) for pk in pk_list]

        if isinstance(value, Model):
            value = [value]

        return [LazyModel.get_identifier(item) for item in value]

    def convert(self, value):
        """Returns a fake manager object for accessing the lazy objects."""
        values = (LazyModel(item) for item in value or [])
        return ManyToManyManager(*values)


class SearchResultImage(str):
    """
    An object that represents a models.ImageField instance.
    It supports some features of a normal ImageField, but not everything.

    """

    storage = default_storage

    def __init__(self, name):
        self.name = name or ''

    @property
    def path(self):
        if self.name:
            return self.storage.path(self.name)
        else:
            return ''

    @property
    def url(self):
        if self.name:
            return self.storage.url(self.name)
        else:
            return ''


class ImageField(CharField):
    """
    A field for use with Django's models.ImageField fields.
    It replicates some of its functionality (url and path properties).

    """

    def convert(self, value):
        return SearchResultImage(value)


class LatLongField(SearchField):

    field_type = 'location'

    def prepare(self, obj):

        value = SearchField.prepare(self, obj)

        if value is None:
            return None

        point = point_from_lat_long(value)
        return '%s,%s' % (point.y, point.x)

    def convert(self, value):

        if value is None:
            return None

        point = point_from_lat_long(value)
        return (point.y, point.x)


class MultiDateTimeField(MultiValueField):

    convert_datetime = DateTimeField().convert

    field_type = 'datetime'

    def __init__(self, **kwargs):
        if kwargs.get('facet_class') is None:
            kwargs['facet_class'] = FacetMultiDateTimeField
        super(MultiDateTimeField, self).__init__(**kwargs)

    def convert(self, value):

        if value is None:
            return None

        return [self.convert_datetime(item) for item in value]


class FacetMultiDateTimeField(FacetField, MultiDateTimeField):
    pass
