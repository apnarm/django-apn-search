"""
A management command to partially update the search index, because we don't
really need half a million stories in our local development environments.

Use the other "update_index" command if a full index is required.
This one is a helper tool for local development purposes.

Usage:

    apnshell partial_index
    apnshell partial_index media notices events.event

    The usage is the same as Haystack's "update_index" management command.


Configuration:

    This is handled in settings.HAYSTACK_PARTIAL_INDEX_OPTIONS
    It must be a dictionary containing
        {
            'app_label.model_name': 'limit_definition'
        }

    Supported limit types:

        * all           Indexes everything according to the IDs found in the
                        database and the search engine. This will index every
                        object the first time it runs, and then only new objects
                        afterwards. It does not detect changes to objects.

        * new           Same as "all", but it indexes objects in the reverse
                        order - most recent ID first.

                        Haystack indexes objects in order of their ID, in
                        ascending order, so this can have weird side effects
                        if the command does not complete successfully; the
                        remaining items it was meant to index will never be
                        indexed by this command, because it assumes all items
                        between the lowest ID and the highest ID have already
                        been indexed.

                        In short, only use this on indexes where a full set of
                        results is not very important.

    Setting limits:

        * all:100       The "all" and "new" directives can optionally have
          new:500       a limit, which will limit the number of items to be
                        indexed per run. Repeated runs of this management
                        command will progressively populate indexes which are
                        using this configuration.

"""

import functools

from optparse import make_option

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import smart_str

from haystack.management.commands.update_index import Command as UpdateCommand

from apn_search.query import SearchQuerySet
from apn_search.utils.indexes import get_unified_index
from apn_search.utils.shell import default_text_color, do_not_print, green_text


class Command(UpdateCommand):

    # Set the default verbosity to 2 because it's nice to see progress.
    option_list = [option for option in UpdateCommand.option_list if option.dest != 'verbosity']
    option_list.append(
        make_option(
            '-v',
            '--verbosity',
            action='store',
            dest='verbosity',
            default='2',
            type='choice',
            choices=['0', '1', '2'],
            help='Verbosity level; 0=minimal output, 1=normal output, 2=all output',
        ),
    )

    def handle(self, *items, **options):

        index_options = getattr(settings, 'HAYSTACK_PARTIAL_INDEX_OPTIONS', None)
        if not index_options:
            raise ImproperlyConfigured('settings.HAYSTACK_PARTIAL_INDEX_OPTIONS is not defined.')

        unified_index = get_unified_index(build=True)

        # Update all of the indexes when no apps/models have been specified.
        # The superclass does this differently and gets duplicates.
        if not items:
            items = set()
            models = unified_index.indexes.keys()
            for model in models:
                items.add(model_label(model))
            items = tuple(sorted(items))

        # Decorate the index_queryset method of any index that has a limit
        # defined in settings.HAYSTACK_PARTIAL_INDEX_OPTIONS
        for model, index in unified_index.indexes.iteritems():
            label = model_label(model)
            limit_definition = index_options.get(label)
            if limit_definition:
                index.index_queryset = limit_index_queryset(
                    index_queryset=index.index_queryset,
                    limit_definition=limit_definition,
                    verbosity=int(options['verbosity']),
                )

        if not options.get('batchsize'):
            options['batchsize'] = 500

        # Now run the update_index command as usual.
        with do_not_print(r'Skipping .+ - no index.'):
            with green_text:
                return super(Command, self).handle(*items, **options)


def model_label(model):
    return '%s.%s' % (model._meta.app_label, model._meta.module_name)


def limit_index_queryset(index_queryset, limit_definition, verbosity):
    """
    Limit the items returned by an index_queryset method.

    The specific rules are defined in
    settings.HAYSTACK_PARTIAL_INDEX_OPTIONS

    """

    limit_value = ':' in limit_definition and limit_definition.split(':', 1)[1]

    @functools.wraps(index_queryset)
    def limited_queryset(*args, **kwargs):

        queryset = index_queryset(*args, **kwargs)

        if verbosity >= 2:
            with default_text_color:
                print 'Limiting %s to %s.' % (
                    smart_str(queryset.model._meta.verbose_name_plural),
                    limit_definition,
                )

        if limit_definition.startswith('new'):
            queryset = new_queryset(queryset, limit=limit_value, index_order='-id')
        elif limit_definition.startswith('all'):
            queryset = new_queryset(queryset, limit=limit_value, index_order='id')
        else:
            raise ImproperlyConfigured(
                'Incorrect limit value %r in '
                'settings.HAYSTACK_PARTIAL_INDEX_OPTIONS' % limit_definition
            )

        return queryset

    return limited_queryset


def new_queryset(queryset, limit, index_order):
    first_id = find_indexed_id(queryset.model, oldest=True)
    last_id = find_indexed_id(queryset.model, newest=True)
    if first_id and last_id:
        queryset = queryset.exclude(id__range=(first_id, last_id))
    if limit:
        limit = int(limit)
        new_ids = queryset.order_by(index_order).values_list('id', flat=True)[:limit]
        queryset = queryset.model.objects.filter(id__in=new_ids)
    return queryset


def find_indexed_id(model, newest=None, oldest=None):
    assert (newest or oldest) and not (newest and oldest)
    prefix = newest and '-' or ''
    search = SearchQuerySet().models(model).direct(**{
        'sort': {
            '_script': {
                'type': 'number',
                'script': prefix + "org.elasticsearch.common.primitives.Ints.tryParse(doc['django_id'].value)",
            }
        }
    })
    try:
        return search[0].id
    except IndexError:
        return None
