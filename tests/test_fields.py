""" Tests for dynamo3.fields """
import unittest

from dynamo3 import DynamoKey, GlobalIndex, LocalIndex, Table, Throughput


class TestEqHash(unittest.TestCase):

    """Tests for equality and hash methods"""

    def test_dynamo_key_eq(self):
        """Dynamo keys should be equal if names are equal"""
        a, b = DynamoKey("foo"), DynamoKey("foo")
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertFalse(a != b)

    def test_local_index_eq(self):
        """Local indexes should be equal"""
        range_key = DynamoKey("foo")
        a, b = LocalIndex.all("a", range_key), LocalIndex.all("a", range_key)
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertFalse(a != b)

    def test_global_index_eq(self):
        """Global indexes should be equal"""
        hash_key = DynamoKey("foo")
        a, b = GlobalIndex.all("a", hash_key), GlobalIndex.all("a", hash_key)
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertFalse(a != b)

    def test_global_local_ne(self):
        """Global indexes should not equal local indexes"""
        field = DynamoKey("foo")
        a, b = LocalIndex.all("a", field), GlobalIndex.all("a", field, field)
        self.assertNotEqual(a, b)

    def test_throughput_eq(self):
        """Throughputs should be equal"""
        a, b = Throughput(), Throughput()
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertFalse(a != b)

    def test_throughput_repr(self):
        """Throughput repr should wrap read/write values"""
        a = Throughput(1, 1)
        self.assertEqual(repr(a), "Throughput(1, 1)")

    def test_table_eq(self):
        """Tables should be equal"""
        field = DynamoKey("foo")
        a, b = Table("a", field), Table("a", field)
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        self.assertFalse(a != b)
