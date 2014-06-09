from haystack.utils.geo import Point, Distance


def point_from_lat_long(value):
    if isinstance(value, Point):
        return value
    if isinstance(value, basestring):
        lat, lng = value.split(',')
    elif isinstance(value, (list, tuple)):
        lat, lng = value
    else:
        raise ValueError("I don't know what to do with this.")
    return Point(float(lng), float(lat))


def point_from_long_lat(value):
    if isinstance(value, Point):
        return value
    if isinstance(value, basestring):
        lng, lat = value.split(',')
    elif isinstance(value, (list, tuple)):
        lng, lat = value
    else:
        raise ValueError("I don't know what to do with this.")
    return Point(float(lng), float(lat))
