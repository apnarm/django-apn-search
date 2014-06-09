def merge_dictionaries(*dictionaries):

    dictionaries = [item for item in dictionaries if item]

    if len(dictionaries) == 1:
        return dictionaries[0]

    result = {}

    for dictionary in dictionaries:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                value = merge_dictionaries(result.get(key, {}), value)
            result[key] = value

    return result
