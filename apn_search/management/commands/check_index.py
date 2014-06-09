import json

from haystack import connections
from haystack.constants import DEFAULT_ALIAS

from django.core.management.base import BaseCommand

from apn_search.utils.mappings import find_conflicts, get_current_mapping, get_existing_mapping
from apn_search.utils.shell import green_text, red_text


def print_bad(message):
    with red_text:
        print message


def print_good(message):
    with green_text:
        print message


def print_dict(dictionary):
    print json.dumps(dictionary, indent=4)


class Command(BaseCommand):

    help = 'Checks the haystack search index.'

    def handle(self, *args, **options):

        backend = connections[DEFAULT_ALIAS].get_backend()
        backend.setup_index_groups()
        for index_name in backend.index_groups:

            if args:
                self.show_fields(index_name, *args)
            else:
                self.find_conflicts(index_name)

    def find_conflicts(self, index_name):

        existing_mappings = get_existing_mapping(index_name=index_name)
        new_mappings = get_current_mapping(index_name=index_name)

        conflicts = tuple(find_conflicts(existing_mappings, new_mappings))

        if conflicts:
            print

        for field_name in conflicts:
            print_bad("'%s.%s' has changed!" % (index_name, field_name))
            print
            print 'Before:'
            print_dict(existing_mappings['modelresult']['properties'][field_name])
            print 'After:'
            print_dict(new_mappings['modelresult']['properties'][field_name])
            print

    def show_fields(self, index_name, *fields):

        existing_mappings = get_existing_mapping(index_name=index_name)
        new_mappings = get_current_mapping(index_name=index_name)

        conflicts = tuple(find_conflicts(existing_mappings, new_mappings))

        for field_name in fields:

            try:
                before = existing_mappings['modelresult']['properties'][field_name]
            except KeyError:
                before = None

            try:
                after = new_mappings['modelresult']['properties'][field_name]
            except KeyError:
                after = None

            if before or after:

                if field_name in conflicts:
                    print_method = print_bad
                else:
                    print_method = print_good

                print_method('Mappings for %s.%s:' % (index_name, field_name))
                print
                if before:
                    print 'Before:'
                    print_dict(before)
                    print
                if after and after != before:
                    print 'After:'
                    print_dict(after)
                    print

