""" Tests for Dynamo3 """
from __future__ import unicode_literals

import six
from botocore.exceptions import ClientError
from decimal import Decimal
from mock import patch
from six.moves.cPickle import dumps, loads  # pylint: disable=F0401,E0611
from six.moves.urllib.parse import urlparse  # pylint: disable=F0401,E0611

from dynamo3 import (DynamoDBConnection, Binary, DynamoKey, Dynamizer, STRING,
                     ThroughputException, Table, GlobalIndex, DynamoDBError)


try:
    import unittest2 as unittest  # pylint: disable=F0401
except ImportError:
    import unittest


if six.PY3:
    unittest.TestCase.assertItemsEqual = unittest.TestCase.assertCountEqual


def is_number(value):
    """ Check if a value is a float or int """
    return isinstance(value, float) or isinstance(value, six.integer_types)


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


class TestMisc(BaseSystemTest):

    """ Tests that don't fit anywhere else """

    def test_connection_host(self):
        """ Connection can access host of endpoint """
        urlparse(self.dynamo.host)

    def test_connection_region(self):
        """ Connection can access name of connected region """
        self.assertTrue(isinstance(self.dynamo.region, six.string_types))

    def test_connect_to_region_old(self):
        """ Can connect to a dynamo region """
        conn = DynamoDBConnection.connect_to_region('us-west-1')
        self.assertIsNotNone(conn.host)

    def test_connect_to_region_creds_old(self):
        """ Can connect to a dynamo region with credentials """
        conn = DynamoDBConnection.connect_to_region(
            'us-west-1', access_key='abc', secret_key='12345')
        self.assertIsNotNone(conn.host)

    def test_connect_to_host_without_session_old(self):
        """ Can connect to a dynamo host without passing in a session """
        conn = DynamoDBConnection.connect_to_host(access_key='abc',
                                                  secret_key='12345')
        self.assertIsNotNone(conn.host)

    def test_connect_to_region(self):
        """ Can connect to a dynamo region """
        conn = DynamoDBConnection.connect('us-west-1')
        self.assertIsNotNone(conn.host)

    def test_connect_to_region_creds(self):
        """ Can connect to a dynamo region with credentials """
        conn = DynamoDBConnection.connect(
            'us-west-1', access_key='abc', secret_key='12345')
        self.assertIsNotNone(conn.host)

    def test_connect_to_host_without_session(self):
        """ Can connect to a dynamo host without passing in a session """
        conn = DynamoDBConnection.connect('us-west-1', host='localhost')
        self.assertIsNotNone(conn.host)

    @patch('dynamo3.connection.time')
    def test_retry_on_throughput_error(self, time):
        """ Throughput exceptions trigger a retry of the request """
        def call(*_, **__):
            """ Dummy service call """
            response = {
                'ResponseMetadata': {
                    'HTTPStatusCode': 400,
                },
                'Error': {
                    'Code': 'ProvisionedThroughputExceededException',
                    'Message': 'Does not matter',
                }
            }
            raise ClientError(response, 'list_tables')

        with patch.object(self.dynamo, 'client') as client:
            client.list_tables.side_effect = call
            with self.assertRaises(ThroughputException):
                self.dynamo.call('list_tables')
        self.assertEqual(len(time.sleep.mock_calls),
                         self.dynamo.request_retries - 1)
        self.assertTrue(time.sleep.called)

    def test_describe_missing(self):
        """ Describing a missing table returns None """
        ret = self.dynamo.describe_table('foobar')
        self.assertIsNone(ret)

    def test_magic_table_props(self):
        """ Table magically looks up properties on response object """
        hash_key = DynamoKey('id')
        self.dynamo.create_table('foobar', hash_key=hash_key)
        ret = self.dynamo.describe_table('foobar')
        self.assertIsNotNone(ret.item_count)
        with self.assertRaises(AttributeError):
            self.assertIsNotNone(ret.crazy_property)

    def test_magic_index_props(self):
        """ Index magically looks up properties on response object """
        index = GlobalIndex.all('idx-name', DynamoKey('id'))
        index.response = {
            'FooBar': 2
        }
        self.assertEqual(index.foo_bar, 2)
        with self.assertRaises(AttributeError):
            self.assertIsNotNone(index.crazy_property)

    def test_describe_during_delete(self):
        """ Describing a table during a delete operation should not crash """
        response = {
            'ItemCount': 0,
            'ProvisionedThroughput': {
                'NumberOfDecreasesToday': 0,
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
            'TableName': 'myTableName',
            'TableSizeBytes': 0,
            'TableStatus': 'DELETING'
        }
        table = Table.from_response(response)
        self.assertEqual(table.status, 'DELETING')

    def test_delete_missing(self):
        """ Deleting a missing table returns False """
        ret = self.dynamo.delete_table('foobar')
        self.assertTrue(not ret)

    def test_delete_dry_run(self):
        """ Delete table dry_run=True """
        ret = self.dynamo.delete_table('foobar', dry_run=True)
        self.assertEqual(ret, {'TableName': 'foobar'})

    def test_connection_version(self):
        """ Using a version will patch the old methods """
        conn = DynamoDBConnection(self.dynamo.client)
        conn.use_version(1)
        self.assertNotEqual(conn.put_item, conn.put_item2)
        conn.use_version(2)
        self.assertEqual(conn.put_item, conn.put_item2)

    def test_re_raise(self):
        """ DynamoDBError can re-raise itself if missing exc_info """
        err = DynamoDBError(400, Code='ErrCode', Message='Ouch', args={})
        try:
            err.re_raise()
            self.assertTrue(False)
        except DynamoDBError as e:
            self.assertEqual(err, e)


class TestDataTypes(BaseSystemTest):

    """ Tests for Dynamo data types """

    def make_table(self):
        """ Convenience method for making a table """
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

    def test_decimal(self):
        """ Store and retrieve a Decimal """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a', 'num': Decimal('1.1')})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['num'], Decimal('1.1'))

    def test_binary(self):
        """ Store and retrieve a binary """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'a', 'data': Binary('abc')})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['data'].value, b'abc')

    def test_binary_bytes(self):
        """ Store and retrieve bytes as a binary """
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
        self.assertFalse(Binary('a') != Binary('a'))

    def test_binary_repr(self):
        """ Binary repr should wrap the contained value """
        self.assertEqual(repr(Binary('a')), 'Binary(%s)' % b'a')

    def test_binary_converts_unicode(self):
        """ Binary will convert unicode to bytes """
        b = Binary('a')
        self.assertTrue(isinstance(b.value, six.binary_type))

    def test_binary_force_string(self):
        """ Binary must wrap a string type """
        with self.assertRaises(TypeError):
            Binary(2)

    def test_bool(self):
        """ Store and retrieve a boolean """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'abc', 'b': True})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['b'], True)
        self.assertTrue(isinstance(item['b'], bool))

    def test_list(self):
        """ Store and retrieve a list """
        self.make_table()
        self.dynamo.put_item('foobar', {'id': 'abc', 'l': ['a', 1, False]})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['l'], ['a', 1, False])

    def test_dict(self):
        """ Store and retrieve a dict """
        self.make_table()
        data = {
            'i': 1,
            's': 'abc',
            'n': None,
            'l': ['a', 1, True],
            'b': False,
        }
        self.dynamo.put_item('foobar', {'id': 'abc', 'd': data})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['d'], data)

    def test_nested_dict(self):
        """ Store and retrieve a nested dict """
        self.make_table()
        data = {
            's': 'abc',
            'd': {
                'i': 42,
            },
        }
        self.dynamo.put_item('foobar', {'id': 'abc', 'd': data})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['d'], data)

    def test_nested_list(self):
        """ Store and retrieve a nested list """
        self.make_table()
        data = [
            1,
            [
                True,
                None,
                'abc',
            ],
        ]
        self.dynamo.put_item('foobar', {'id': 'abc', 'l': data})
        item = list(self.dynamo.scan('foobar'))[0]
        self.assertEqual(item['l'], data)

    def test_unrecognized_type(self):
        """ Dynamizer throws error on unrecognized type """
        value = {
            'ASDF': 'abc',
        }
        with self.assertRaises(TypeError):
            self.dynamo.dynamizer.decode(value)


class TestDynamizer(unittest.TestCase):

    """ Tests for the Dynamizer """

    def test_register_encoder(self):
        """ Can register a custom encoder """
        from datetime import datetime
        dynamizer = Dynamizer()
        dynamizer.register_encoder(datetime, lambda d, v:
                                   (STRING, v.isoformat()))
        now = datetime.utcnow()
        self.assertEqual(dynamizer.raw_encode(now), (STRING, now.isoformat()))

    def test_encoder_missing(self):
        """ If no encoder is found, raise ValueError """
        from datetime import datetime
        dynamizer = Dynamizer()
        with self.assertRaises(ValueError):
            dynamizer.encode(datetime.utcnow())
