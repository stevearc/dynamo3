import six
from dynamo3 import (STRING, NUMBER, BINARY, Binary, DynamoKey, AllIndex,
                     KeysOnlyIndex, IncludeIndex, GlobalAllIndex,
                     GlobalKeysOnlyIndex, GlobalIncludeIndex, Table, Throughput)

try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


if six.PY3:
    unittest.TestCase.assertItemsEqual = unittest.TestCase.assertCountEqual


class BaseSystemTest(unittest.TestCase):

    """ Base class for system tests """
    dynamo = None

    def setUp(self):
        super(BaseSystemTest, self).setUp()
        # Clear out any pre-existing tables
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)

    def tearDown(self):
        super(BaseSystemTest, self).tearDown()
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)


class TestCreate(BaseSystemTest):

    def test_create_hash_table(self):
        """ Create a table with just a hash key """
        hash_key = DynamoKey('id', data_type=STRING)
        table = Table('foobar', hash_key)
        self.dynamo.create_table('foobar', hash_key=hash_key)
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_hash_range_table(self):
        """ Create a table with a hash and range key """
        hash_key = DynamoKey('id', data_type=STRING)
        range_key = DynamoKey('num', data_type=NUMBER)
        table = Table('foobar', hash_key, range_key)
        self.dynamo.create_table('foobar', hash_key, range_key)
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_local_index(self):
        """ Create a table with a local index """
        hash_key = DynamoKey('id', data_type=STRING)
        range_key = DynamoKey('num', data_type=NUMBER)
        index_field = DynamoKey('name')
        index = AllIndex('name-index', index_field)
        table = Table('foobar', hash_key, range_key, [index])
        self.dynamo.create_table(
            'foobar', hash_key, range_key, indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_local_keys_index(self):
        """ Create a table with a local KeysOnly index """
        hash_key = DynamoKey('id', data_type=STRING)
        range_key = DynamoKey('num', data_type=NUMBER)
        index_field = DynamoKey('name')
        index = KeysOnlyIndex('name-index', index_field)
        table = Table('foobar', hash_key, range_key, [index])
        self.dynamo.create_table(
            'foobar', hash_key, range_key, indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_local_includes_index(self):
        """ Create a table with a local Includes index """
        hash_key = DynamoKey('id', data_type=STRING)
        range_key = DynamoKey('num', data_type=NUMBER)
        index_field = DynamoKey('name')
        index = IncludeIndex('name-index', index_field,
                             includes=['foo', 'bar'])
        table = Table('foobar', hash_key, range_key, [index])
        self.dynamo.create_table(
            'foobar', hash_key, range_key, indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_global_index(self):
        """ Create a table with a global index """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalAllIndex('name-index', index_field)
        table = Table('foobar', hash_key, global_indexes=[index])
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_global_keys_index(self):
        """ Create a table with a global KeysOnly index """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalKeysOnlyIndex('name-index', index_field)
        table = Table('foobar', hash_key, global_indexes=[index])
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_global_includes_index(self):
        """ Create a table with a global Includes index """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalIncludeIndex(
            'name-index', index_field, includes=['foo', 'bar'])
        table = Table('foobar', hash_key, global_indexes=[index])
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_global_hash_range_index(self):
        """ Create a global index with a hash and range key """
        hash_key = DynamoKey('id', data_type=STRING)
        index_hash = DynamoKey('foo')
        index_range = DynamoKey('bar')
        index = GlobalAllIndex('foo-index', index_hash, index_range)
        table = Table('foobar', hash_key, global_indexes=[index])
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_table_throughput(self):
        """ Create a table and set throughput """
        hash_key = DynamoKey('id', data_type=STRING)
        throughput = Throughput(8, 2)
        table = Table('foobar', hash_key, throughput=throughput)
        self.dynamo.create_table(
            'foobar', hash_key=hash_key, throughput=throughput)
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)

    def test_create_global_index_throughput(self):
        """ Create a table and set throughput on global index """
        hash_key = DynamoKey('id', data_type=STRING)
        throughput = Throughput(8, 2)
        index_field = DynamoKey('name')
        index = GlobalAllIndex(
            'name-index', index_field, throughput=throughput)
        table = Table('foobar', hash_key, global_indexes=[index])
        self.dynamo.create_table(
            'foobar', hash_key=hash_key, global_indexes=[index])
        desc = self.dynamo.describe_table('foobar')
        self.assertEqual(desc, table)
