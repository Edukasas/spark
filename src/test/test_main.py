import unittest
from src.main.python.main import get_coordinates, calculate_geohash
import pygeohash as pgh

class TestGeolocationFunctions(unittest.TestCase):

    def test_get_coordinates_valid(self):
        """Test valid address lookup"""
        lat, lon = get_coordinates("New York, USA")
        self.assertIsNotNone(lat)
        self.assertIsNotNone(lon)
        self.assertTrue(-90 <= lat <= 90)
        self.assertTrue(-180 <= lon <= 180)

    def test_get_coordinates_invalid(self):
        """Test invalid address lookup"""
        lat, lon = get_coordinates("SomeNonExistentPlace123")
        self.assertIsNone(lat)
        self.assertIsNone(lon)

    def test_calculate_geohash_valid(self):
        """Test geohash encoding for valid lat/lon"""
        geohash = calculate_geohash(40.7128, -74.0060, precision=4)
        expected = pgh.encode(40.7128, -74.0060, precision=4)
        self.assertEqual(geohash, expected)

    def test_calculate_geohash_invalid(self):
        """Test geohash for invalid lat/lon"""
        self.assertIsNone(calculate_geohash(200, -74.0060))  # Invalid latitude
        self.assertIsNone(calculate_geohash(40.7128, -200))  # Invalid longitude
        self.assertIsNone(calculate_geohash(None, None))  # None values
        self.assertIsNone(calculate_geohash("abc", "xyz"))  # Non-numeric values

if __name__ == "__main__":
    unittest.main()
