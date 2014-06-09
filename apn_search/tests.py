# TODO: enable tests again and make some more

from django.test import TestCase

from apn_search.query import SearchQuerySet
from apn_search.utils.geo import Distance, point_from_lat_long, point_from_long_lat


class LocationQueryTests(TestCase):

    DISABLE_TESTS = True

    def setUp(self):

        # Ensure we have results.
        total = SearchQuerySet().count()
        self.assertTrue(total)

        # Get some real coords from the index
        point = point_from_long_lat((151.9491543, -27.4581498))
        distance = Distance(km=100000000)
        result = (
            SearchQuerySet()
            .dwithin('geoposition', point, distance)
            .distance('geoposition', point)
            .order_by('distance')
        )[0]
        self.point = point_from_lat_long(result.geoposition)

    def test_fake_geo_stuff(self):
        distance = Distance(km=1000)
        query = (
            SearchQuerySet()
            .dwithin('geoposition', self.point, distance)
            .distance('geoposition', self.point)
        )
        self.assertTrue(query.count())

    def test_radius_filter(self):

        # Perform a standard dwithin search and find the number of results.
        distance = Distance(km=1)
        normal_query = (
            SearchQuerySet()
            .dwithin('geoposition', self.point, distance)
        )
        normal_count = normal_query.count()
        self.assertTrue(normal_count)

        # Use the custom method and see that it returns the same number.
        query = (
            SearchQuerySet()
            .radius_filter(0, 1, geoposition=self.point, order_by='distance')
        )
        self.assertEqual(query.count(), normal_count)

    def test_range_search(self):
        counts = set()
        for distance in range(0, 100, 10):
            count = (
                SearchQuerySet()
                .radius_filter(distance, distance + 10, geoposition=self.point)
                .count()
            )
            counts.add(count)
        self.assertTrue(len(counts) > 1)


if LocationQueryTests.DISABLE_TESTS:
    for attr in dir(LocationQueryTests):
        if attr.startswith('test_'):
            delattr(LocationQueryTests, attr)
