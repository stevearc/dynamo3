""" Tests for Dynamo3 """
from __future__ import unicode_literals

import six
from six.moves.urllib.parse import urlparse  # pylint: disable=F0401,E0611
from six.moves.cPickle import dumps, loads  # pylint: disable=F0401,E0611

from dynamo3 import Binary, DynamoKey


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

    def test_describe_missing(self):
        """ Describing a missing table returns None """
        ret = self.dynamo.describe_table('foobar')
        self.assertIsNone(ret)

    def test_delete_missing(self):
        """ Deleting a missing table returns False """
        ret = self.dynamo.delete_table('foobar')
        self.assertTrue(not ret)


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

    def test_binary_converts_unicode(self):
        """ Binary will convert unicode to bytes """
        b = Binary('a')
        self.assertTrue(isinstance(b.value, six.binary_type))

    def test_binary_force_string(self):
        """ Binary must wrap a string type """
        with self.assertRaises(TypeError):
            Binary(2)
