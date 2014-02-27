import six
from dynamo3 import (STRING, NUMBER, BINARY, Binary, DynamoKey, AllIndex,
                     KeysOnlyIndex, IncludeIndex, GlobalAllIndex,
                     GlobalKeysOnlyIndex, GlobalIncludeIndex, Table,
                     Throughput)

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


class TestUpdateTable(BaseSystemTest):

    """  """

    def test_update_table_throughput(self):
        """ Update the table throughput """
        hash_key = DynamoKey('id', data_type=STRING)
        self.dynamo.create_table('foobar', hash_key=hash_key)
        tp = Throughput(2, 1)
        self.dynamo.update_table('foobar', throughput=tp)
        table = self.dynamo.describe_table('foobar')
        self.assertEqual(table.throughput, tp)

    def test_update_global_index_throughput(self):
        """ Update throughput on a global index """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalAllIndex('name-index', index_field)
        self.dynamo.create_table(
            'foobar', hash_key=hash_key, global_indexes=[index])
        tp = Throughput(2, 1)
        self.dynamo.update_table('foobar', global_indexes={'name-index': tp})
        table = self.dynamo.describe_table('foobar')
        self.assertEqual(table.global_indexes[0].throughput, tp)

    def test_update_multiple_throughputs(self):
        """ Update table and global index throughputs """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalAllIndex('name-index', index_field)
        self.dynamo.create_table(
            'foobar', hash_key=hash_key, global_indexes=[index])
        tp = Throughput(2, 1)
        self.dynamo.update_table(
            'foobar', throughput=tp, global_indexes={'name-index': tp})
        table = self.dynamo.describe_table('foobar')
        self.assertEqual(table.throughput, tp)
        self.assertEqual(table.global_indexes[0].throughput, tp)


class TestBatchWrite(BaseSystemTest):

    """  """

    def test_write_items(self):
        """ Batch write items to table """
        hash_key = DynamoKey('id', data_type=STRING)
        self.dynamo.create_table('foobar', hash_key=hash_key)
        with self.dynamo.batch_write('foobar') as batch:
            batch.put_item({'id': 'a'})
        ret = list(self.dynamo.scan('foobar'))
        self.assertItemsEqual(ret, [{'id': 'a'}])

    def test_delete_items(self):
        """ Batch write can delete items from table """
        hash_key = DynamoKey('id', data_type=STRING)
        self.dynamo.create_table('foobar', hash_key=hash_key)
        with self.dynamo.batch_write('foobar') as batch:
            batch.put_item({'id': 'a'})
            batch.put_item({'id': 'b'})
        with self.dynamo.batch_write('foobar') as batch:
            batch.delete_item(id='b')
        ret = list(self.dynamo.scan('foobar'))
        self.assertItemsEqual(ret, [{'id': 'a'}])

    def test_write_many(self):
        """ Can batch write arbitrary numbers of items """
        hash_key = DynamoKey('id', data_type=STRING)
        self.dynamo.create_table('foobar', hash_key=hash_key)
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(50):
                batch.put_item({'id': str(i)})
        count = self.dynamo.scan('foobar', count=True)
        self.assertEqual(count, 50)
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(50):
                batch.delete_item(id=str(i))
        count = self.dynamo.scan('foobar', count=True)
        self.assertEqual(count, 0)


class TestQuery(BaseSystemTest):

    """  """

    def make_table(self):
        hash_key = DynamoKey('id')
        range_key = DynamoKey('num', data_type=NUMBER)
        self.dynamo.create_table('foobar', hash_key=hash_key,
                                 range_key=range_key)

    def test_hash(self):
        """ Can query on the hash key """
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key)
        self.dynamo.put_item('foobar', {'id': 'a'})
        results = self.dynamo.query('foobar', id__eq='a')
        self.assertItemsEqual(list(results), [{'id': 'a'}])

    def test_local_index(self):
        """ Can query on a local index """
        hash_key = DynamoKey('id', data_type=STRING)
        range_key = DynamoKey('num', data_type=NUMBER)
        index_field = DynamoKey('name')
        index = KeysOnlyIndex('name-index', index_field)
        self.dynamo.create_table('foobar', hash_key, range_key,
                                 indexes=[index])
        item = {
            'id': 'a',
            'num': 1,
            'name': 'baz',
        }
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', name__eq='baz',
                                index='name-index')
        self.assertItemsEqual(list(ret), [item])

    def test_global_index(self):
        """ Can query on a global index """
        hash_key = DynamoKey('id', data_type=STRING)
        index_field = DynamoKey('name')
        index = GlobalAllIndex('name-index', index_field)
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])
        item = {
            'id': 'a',
            'name': 'baz',
        }
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', name__eq='baz',
                                index='name-index')
        self.assertItemsEqual(list(ret), [item])

    def test_attributes(self):
        """ Can select only certain attributes """
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key)
        item = {
            'id': 'a',
            'foo': 'bar',
        }
        self.dynamo.put_item('foobar', item)
        results = self.dynamo.query('foobar', attributes=['id'], id__eq='a')
        self.assertItemsEqual(list(results), [{'id': 'a'}])

    def test_order_desc(self):
        """ Can sort the results in descending order """
        self.make_table()
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(3):
                batch.put_item({'id': 'a', 'num': i})
        ret = self.dynamo.query('foobar', attributes=['num'], id__eq='a',
                                desc=True)
        self.assertEqual(list(ret), [{'num': i} for i in range(2, -1, -1)])

    def test_limit(self):
        """ Can limit the number of query results """
        self.make_table()
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(3):
                batch.put_item({'id': 'a', 'num': i})
        ret = self.dynamo.query('foobar', id__eq='a', limit=1)
        self.assertEqual(len(list(ret)), 1)

    def test_count(self):
        """ Can count items instead of returning the actual items """
        self.make_table()
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(3):
                batch.put_item({'id': 'a', 'num': i})
        ret = self.dynamo.query('foobar', count=True, id__eq='a')
        self.assertEqual(ret, 3)

    def test_eq(self):
        """ Can query with EQ constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', item)
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 2})
        ret = self.dynamo.query('foobar', id__eq='a', num__eq=1)
        self.assertItemsEqual(list(ret), [item])

    def test_le(self):
        """ Can query with <= constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', item)
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 2})
        ret = self.dynamo.query('foobar', id__eq='a', num__le=1)
        self.assertItemsEqual(list(ret), [item])

    def test_lt(self):
        """ Can query with < constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', item)
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 2})
        ret = self.dynamo.query('foobar', id__eq='a', num__lt=2)
        self.assertItemsEqual(list(ret), [item])

    def test_ge(self):
        """ Can query with >= constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', num__ge=2)
        self.assertItemsEqual(list(ret), [item])

    def test_gt(self):
        """ Can query with > constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', num__gt=1)
        self.assertItemsEqual(list(ret), [item])

    def test_beginswith(self):
        """ Can query with 'begins with' constraint """
        hash_key = DynamoKey('id')
        range_key = DynamoKey('name')
        self.dynamo.create_table('foobar', hash_key=hash_key,
                                 range_key=range_key)
        item = {'id': 'a', 'name': 'David'}
        self.dynamo.put_item('foobar', {'id': 'a', 'name': 'Steven'})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', name__beginswith='D')
        self.assertItemsEqual(list(ret), [item])

    def test_between(self):
        """ Can query with 'between' constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.query('foobar', id__eq='a', num__between=(2, 10))
        self.assertItemsEqual(list(ret), [item])

    def test_bad_query_op(self):
        """ Malformed query keyword raises error """
        self.make_table()
        with self.assertRaises(TypeError):
            self.dynamo.query('foobar', id__eq='a', num_lt=3)


class TestMisc(BaseSystemTest):

    """  """

    def test_connection_host(self):
        """ Connection can access host of endpoint """
        six.moves.urllib.parse.urlparse(self.dynamo.host)

    def test_connection_region(self):
        """ Connection can access name of connected region """
        self.assertTrue(isinstance(self.dynamo.region, six.string_types))

    def test_describe_missing(self):
        """ Describing a missing table returns None """
        ret = self.dynamo.describe_table('foobar')
        self.assertIsNone(ret)

    def test_delete_missing(self):
        """ Deleting a missing table returns False """
        ret = self.dynamo.delete_table('foobar')
        self.assertTrue(not ret)


class TestScan(BaseSystemTest):

    def make_table(self):
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key=hash_key)

    def test_attributes(self):
        """ Can select only certain attributes """
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key)
        item = {
            'id': 'a',
            'foo': 'bar',
        }
        self.dynamo.put_item('foobar', item)
        results = self.dynamo.scan('foobar', attributes=['id'])
        self.assertItemsEqual(list(results), [{'id': 'a'}])

    def test_limit(self):
        """ Can limit the number of scan results """
        self.make_table()
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(3):
                batch.put_item({'id': str(i)})
        ret = self.dynamo.scan('foobar', limit=1)
        self.assertEqual(len(list(ret)), 1)

    def test_count(self):
        """ Can count items instead of returning the actual items """
        self.make_table()
        with self.dynamo.batch_write('foobar') as batch:
            for i in six.moves.xrange(3):
                batch.put_item({'id': str(i)})
        ret = self.dynamo.scan('foobar', count=True)
        self.assertEqual(ret, 3)

    def test_eq(self):
        """ Can scan with EQ constraint """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a'})
        self.dynamo.put_item('foobar', {'id': 'b'})
        ret = self.dynamo.query('foobar', id__eq='a')
        self.assertItemsEqual(list(ret), [{'id': 'a'}])

    def test_le(self):
        """ Can scan with <= constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', item)
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 2})
        ret = self.dynamo.scan('foobar', num__le=1)
        self.assertItemsEqual(list(ret), [item])

    def test_lt(self):
        """ Can scan with < constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', item)
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 2})
        ret = list(self.dynamo.scan('foobar', num__lt=2))
        self.assertItemsEqual(ret, [item])

    def test_ge(self):
        """ Can scan with >= constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__ge=2)
        self.assertItemsEqual(list(ret), [item])

    def test_gt(self):
        """ Can scan with > constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__gt=1)
        self.assertItemsEqual(list(ret), [item])

    def test_beginswith(self):
        """ Can scan with 'begins with' constraint """
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key=hash_key)
        item = {'id': 'a', 'name': 'David'}
        self.dynamo.put_item('foobar', {'id': 'b', 'name': 'Steven'})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', name__beginswith='D')
        self.assertItemsEqual(list(ret), [item])

    def test_between(self):
        """ Can scan with 'between' constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__between=(2, 10))
        self.assertItemsEqual(list(ret), [item])

    def test_in(self):
        """ Can scan with 'in' constraint """
        self.make_table()
        item = {'id': 'a', 'num': 2}
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__in=(2, 3, 4, 5))
        self.assertItemsEqual(list(ret), [item])

    def test_contains(self):
        """ Can scan with 'contains' constraint """
        self.make_table()
        item = {'id': 'a', 'nums': set([1, 2, 3])}
        self.dynamo.put_item('foobar', {'id': 'b', 'nums': set([4, 5, 6])})
        self.dynamo.put_item('foobar', item)
        ret = list(self.dynamo.scan('foobar', nums__contains=2))
        self.assertItemsEqual(ret, [item])

    def test_ncontains(self):
        """ Can scan with 'not contains' constraint """
        self.make_table()
        item = {'id': 'a', 'nums': set([1, 2, 3])}
        self.dynamo.put_item('foobar', {'id': 'b', 'nums': set([4, 5, 6])})
        self.dynamo.put_item('foobar', item)
        ret = list(self.dynamo.scan('foobar', nums__ncontains=4))
        self.assertItemsEqual(ret, [item])

    def test_is_null(self):
        """ Can scan with 'is null' constraint """
        self.make_table()
        item = {'id': 'a'}
        self.dynamo.put_item('foobar', {'id': 'b', 'num': 1})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__null=True)
        self.assertItemsEqual(list(ret), [item])

    def test_is_not_null(self):
        """ Can scan with 'is not null' constraint """
        self.make_table()
        item = {'id': 'a', 'num': 1}
        self.dynamo.put_item('foobar', {'id': 'b'})
        self.dynamo.put_item('foobar', item)
        ret = self.dynamo.scan('foobar', num__null=False)
        self.assertItemsEqual(list(ret), [item])


class TestUpdateItem(BaseSystemTest):
    # TODO: (stevearc 2014-02-27)
    pass


class TestPutItem(BaseSystemTest):
    # TODO: (stevearc 2014-02-27)
    pass


class TestBatchGet(BaseSystemTest):
    # TODO: (stevearc 2014-02-27)
    pass


class TestGetItem(BaseSystemTest):
    # TODO: (stevearc 2014-02-27)
    pass


class TestDataTypes(BaseSystemTest):

    def make_table(self):
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key=hash_key)

    def test_string(self):
        """ Store and retrieve a string """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'abc'})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['id'], 'abc')
        self.assertTrue(isinstance(item['id'], six.text_type))

    def test_int(self):
        """ Store and retrieve an int """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 1})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['num'], 1)

    def test_float(self):
        """ Store and retrieve a float """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a', 'num': 1.1})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertAlmostEqual(float(item['num']), 1.1)

    def test_binary(self):
        """ Store and retrieve a binary """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a', 'data': Binary('abc')})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['data'].value, 'abc')

    def test_binary_bytes(self):
        """ Store and retrieve bytes as a binary """
        from six.moves.cPickle import dumps, loads
        self.make_table()
        data = {'a': 1, 'b': 2}
        self.dynamo.put_item('foobar', {'id': 'a',
                                        'data': Binary(dumps(data))})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(loads(item['data'].value), data)

    def test_string_set(self):
        """ Store and retrieve a string set """
        self.make_table()
        item = {
            'id': 'a',
            'datas': set(['a', 'b']),
        }
        self.dynamo.put_item('foobar', item)
        ret = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(ret, item)

    def test_number_set(self):
        """ Store and retrieve a number set """
        self.make_table()
        item = {
            'id': 'a',
            'datas': set([1, 2, 3]),
        }
        self.dynamo.put_item('foobar', item)
        ret = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(ret, item)

    def test_binary_set(self):
        """ Store and retrieve a binary set """
        self.make_table()
        item = {
            'id': 'a',
            'datas': set([Binary('a'), Binary('b')]),
        }
        self.dynamo.put_item('foobar', item)
        ret = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(ret, item)

    def test_binary_equal(self):
        """ Binary should eq other Binaries and also raw bytestrings """
        self.assertEqual(Binary('a'), Binary('a'))
        self.assertEqual(Binary('a'), b'a')

    def test_binary_converts_unicode(self):
        """ Binary will convert unicode to bytes """
        b = Binary(u'a')
        self.assertTrue(isinstance(b.value, six.binary_type))

    def test_binary_force_string(self):
        """ Binary must wrap a string type """
        with self.assertRaises(TypeError):
            Binary(2)
