from haystack import connections
from haystack.constants import DEFAULT_ALIAS


def get_existing_mapping(using=DEFAULT_ALIAS, index_name=None):
    backend = connections[using].get_backend()
    if index_name is None:
        index_name = backend.index_name
    mappings = backend.conn.get_mapping(indexes=[index_name])
    if backend.new_version:
        # Newer ES versions return everything wrapped in a 'mappings' object.
        # It is kind of redundant for the result of this function, so unwrap it.
        return mappings[index_name]['mappings']
    else:
        return mappings[index_name]


def get_current_mapping(using=DEFAULT_ALIAS, index_name=None):

    engine = connections[using]

    backend = engine.get_backend()

    if index_name:
        backend.setup_index_groups()
        indexes = backend.index_groups.get(index_name, [])
        isolated_index = engine.unified_index()
        isolated_index.build(indexes=indexes)
        fields = isolated_index.all_searchfields()
    else:
        unified_index = engine.get_unified_index()
        fields = unified_index.all_searchfields()

    content_field_name, field_mapping = backend.build_schema(fields)

    current_mapping = {
        'modelresult': {
            'properties': field_mapping
        }
    }

    return current_mapping


EQUIVALENT_VALUES = tuple(set(items) for items in (
    ('no', None),
    ('yes', True),
))


def find_conflicts(old_mappings, new_mappings):

    if not old_mappings or not new_mappings:
        return

    old_mappings = old_mappings['modelresult']['properties']
    new_mappings = new_mappings['modelresult']['properties']

    shared_fields = set(old_mappings.keys()).intersection(new_mappings.keys())

    for field_name in shared_fields:

        existing_mapping = old_mappings[field_name]
        new_mapping = new_mappings[field_name]

        for prop_name in ('analyzer', 'store', 'type'):

            new_prop_value = new_mapping.get(prop_name)
            old_prop_value = existing_mapping.get(prop_name)

            if new_prop_value != old_prop_value:

                if set((old_prop_value, new_prop_value)) in EQUIVALENT_VALUES:
                    continue

                yield field_name
