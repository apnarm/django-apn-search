from django.core.exceptions import ImproperlyConfigured
from django.core.management.base import LabelCommand
from django.db.models import get_app, get_models, get_model
from django.utils.encoding import smart_str

from haystack.exceptions import NotHandled
from haystack.management.commands.update_index import APP, MODEL

from apn_search.query import SearchQuerySet
from apn_search.utils.indexes import get_backend, get_index, get_unified_index


class Command(LabelCommand):

    def handle(self, *items, **options):

        self.verbosity = int(options['verbosity'])

        # Clean all of the indexes when no apps/models have been specified.
        # The superclass does this differently and gets duplicates.
        if not items:
            items = set()
            unified_index = get_unified_index()
            unified_index.build()
            models = unified_index.indexes.keys()
            for model in models:
                items.add(model_label(model))
            items = tuple(sorted(items))

        return super(Command, self).handle(*items, **options)

    def handle_label(self, label, **options):
        for model in get_models_from_label(label):
            try:
                index = get_index(model)
            except NotHandled:
                if self.verbosity >= 2:
                    print "Skipping '%s' - no index." % model
            else:
                self.handle_model(model, index)

    def handle_model(self, model, index):

        if self.verbosity >= 1:
            print "Checking %s for leftovers." % smart_str(model._meta.verbose_name_plural)

        if self.verbosity >= 1:
            print 'Getting database ids...'
        db_ids = index.index_queryset().values_list('id', flat=True)
        db_ids = set(smart_str(pk) for pk in db_ids)

        if self.verbosity >= 1:
            print 'Getting indexed ids...'
        model_search_results = SearchQuerySet().models(model)
        show_progress = self.verbosity >= 2
        indexed_ids = model_search_results.get_django_ids(verbose=show_progress)
        indexed_ids = set(indexed_ids)

        leftovers = indexed_ids - db_ids
        count = len(leftovers)

        if count:
            if self.verbosity >= 1:
                print 'Removing %d leftover document%s.' % (count, count != 1 and 's' or '')
            self.remove_leftovers(model, leftovers)
        else:
            if self.verbosity >= 2:
                print 'Nothing to remove.'

    def remove_leftovers(self, model, pks):
        backend = get_backend()
        label = model_label(model)
        for pk in pks:
            document_id = '%s.%s' % (label, pk)
            if self.verbosity >= 2:
                print 'Removing %s' % document_id
            backend.remove(document_id)


def get_models_from_label(label):
    app_or_model = is_app_or_model(label)
    if app_or_model == APP:
        app_mod = get_app(label)
        return get_models(app_mod)
    else:
        app_label, model_name = label.split('.')
        return [get_model(app_label, model_name)]


def is_app_or_model(label):
    label_bits = label.split('.')
    if len(label_bits) == 1:
        return APP
    elif len(label_bits) == 2:
        return MODEL
    else:
        raise ImproperlyConfigured(
            "'%s' isn't recognized as an app (<app_label>) "
            "or model (<app_label>.<model_name>)." % label
        )


def model_label(model):
    return '%s.%s' % (model._meta.app_label, model._meta.module_name)
