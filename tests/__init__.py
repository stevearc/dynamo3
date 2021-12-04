""" Tests for Dynamo3 """

import sys
import unittest
from decimal import Decimal
from pickle import dumps, loads
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from mock import ANY, MagicMock, patch

from dynamo3 import (
    Binary,
    Dynamizer,
    DynamoDBConnection,
    DynamoDBError,
    DynamoKey,
    GlobalIndex,
    Limit,
    Table,
    ThroughputException,
)
from dynamo3.constants import STRING
from dynamo3.result import Capacity, ConsumedCapacity, Count, ResultSet, add_dicts


class BaseSystemTest(unittest.TestCase):

    """Base class for system tests"""

    dynamo: DynamoDBConnection = None  # type: ignore

    def setUp(self):
        super(BaseSystemTest, self).setUp()
        # Clear out any pre-existing tables
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)

    def tearDown(self):
        super(BaseSystemTest, self).tearDown()
        for tablename in self.dynamo.list_tables():
            self.dynamo.delete_table(tablename)
        self.dynamo.clear_hooks()


class TestMisc(BaseSystemTest):

    """Tests that don't fit anywhere else"""

    def tearDown(self):
        super(TestMisc, self).tearDown()
        self.dynamo.default_return_capacity = False

    def test_connection_host(self):
        """Connection can access host of endpoint"""
        urlparse(self.dynamo.host)

    def test_connection_region(self):
        """Connection can access name of connected region"""
        self.assertTrue(isinstance(self.dynamo.region, str))

    def test_connect_to_region(self):
        """Can connect to a dynamo region"""
        conn = DynamoDBConnection.connect("us-west-1")
        self.assertIsNotNone(conn.host)

    def test_connect_to_region_creds(self):
        """Can connect to a dynamo region with credentials"""
        conn = DynamoDBConnection.connect(
            "us-west-1", access_key="abc", secret_key="12345"
        )
        self.assertIsNotNone(conn.host)

    def test_connect_to_host_without_session(self):
        """Can connect to a dynamo host without passing in a session"""
        conn = DynamoDBConnection.connect("us-west-1", host="localhost")
        self.assertIsNotNone(conn.host)

    @patch("dynamo3.connection.time")
    def test_retry_on_throughput_error(self, time):
        """Throughput exceptions trigger a retry of the request"""

        def call(*_, **__):
            """Dummy service call"""
            response = {
                "ResponseMetadata": {
                    "HTTPStatusCode": 400,
                },
                "Error": {
                    "Code": "ProvisionedThroughputExceededException",
                    "Message": "Does not matter",
                },
            }
            raise ClientError(response, "list_tables")

        with patch.object(self.dynamo, "client") as client:
            client.list_tables.side_effect = call
            with self.assertRaises(ThroughputException):
                self.dynamo.call("list_tables")
        self.assertEqual(len(time.sleep.mock_calls), self.dynamo.request_retries - 1)
        self.assertTrue(time.sleep.called)

    def test_describe_missing(self):
        """Describing a missing table returns None"""
        ret = self.dynamo.describe_table("foobar")
        self.assertIsNone(ret)

    def test_magic_table_props(self):
        """Table can look up properties on response object"""
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)
        ret = self.dynamo.describe_table("foobar")
        assert ret is not None
        self.assertEqual(ret.item_count, ret["ItemCount"])
        with self.assertRaises(KeyError):
            self.assertIsNotNone(ret["Missing"])

    def test_magic_index_props(self):
        """Index can look up properties on response object"""
        index = GlobalIndex.all("idx-name", DynamoKey("id"))
        index.response = {"FooBar": 2}
        self.assertEqual(index["FooBar"], 2)
        with self.assertRaises(KeyError):
            self.assertIsNotNone(index["Missing"])

    def test_describe_during_delete(self):
        """Describing a table during a delete operation should not crash"""
        response = {
            "ItemCount": 0,
            "ProvisionedThroughput": {
                "NumberOfDecreasesToday": 0,
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
            "TableName": "myTableName",
            "TableSizeBytes": 0,
            "TableStatus": "DELETING",
        }
        table = Table.from_response(response)
        self.assertEqual(table.status, "DELETING")

    def test_delete_missing(self):
        """Deleting a missing table returns False"""
        ret = self.dynamo.delete_table("foobar")
        self.assertTrue(not ret)

    def test_re_raise_passthrough(self):
        """DynamoDBError can re-raise itself if missing original exception"""
        err = DynamoDBError(400, Code="ErrCode", Message="Ouch", args={})
        caught = False
        try:
            err.re_raise()
        except DynamoDBError as e:
            caught = True
            self.assertEqual(err, e)
        self.assertTrue(caught)

    def test_re_raise(self):
        """DynamoDBError can re-raise itself with stacktrace of original exc"""
        caught = False
        try:
            try:
                raise Exception("Hello")
            except Exception as e1:
                err = DynamoDBError(
                    400,
                    Code="ErrCode",
                    Message="Ouch",
                    args={},
                    exc_info=sys.exc_info(),
                )
                err.re_raise()
        except DynamoDBError as e:
            caught = True
            import traceback

            tb = traceback.format_tb(e.__traceback__)
            self.assertIn("Hello", tb[-1])
            self.assertEqual(e.status_code, 400)
        self.assertTrue(caught)

    def test_default_return_capacity(self):
        """When default_return_capacity=True, always return capacity"""
        self.dynamo.default_return_capacity = True
        with patch.object(self.dynamo, "call") as call:
            call().get.return_value = None
            rs = self.dynamo.scan("foobar")
            list(rs)
        call.assert_called_with(
            "scan",
            TableName="foobar",
            ReturnConsumedCapacity="INDEXES",
            ConsistentRead=False,
        )

    def test_list_tables_page(self):
        """Call to ListTables should page results"""
        hash_key = DynamoKey("id")
        for i in range(120):
            self.dynamo.create_table("table%d" % i, hash_key=hash_key)
        tables = list(self.dynamo.list_tables(110))
        self.assertEqual(len(tables), 110)

    def test_limit_complete(self):
        """A limit with item_capacity = 0 is 'complete'"""
        limit = Limit(item_limit=0)
        self.assertTrue(limit.complete)

    def test_wait_create_table(self):
        """Create table shall wait for the table to come online."""
        tablename = "foobar_wait"
        hash_key = DynamoKey("id")
        self.dynamo.create_table(tablename, hash_key=hash_key, wait=True)
        self.assertIsNotNone(self.dynamo.describe_table(tablename))

    def test_wait_delete_table(self):
        """Delete table shall wait for the table to go offline."""
        tablename = "foobar_wait"
        hash_key = DynamoKey("id")
        self.dynamo.create_table(tablename, hash_key=hash_key, wait=True)
        result = self.dynamo.delete_table(tablename, wait=True)
        self.assertTrue(result)


class TestDataTypes(BaseSystemTest):

    """Tests for Dynamo data types"""

    def make_table(self):
        """Convenience method for making a table"""
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)

    def test_string(self):
        """Store and retrieve a string"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "abc"})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["id"], "abc")
        self.assertTrue(isinstance(item["id"], str))

    def test_int(self):
        """Store and retrieve an int"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["num"], 1)

    def test_float(self):
        """Store and retrieve a float"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1.1})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertAlmostEqual(float(item["num"]), 1.1)

    def test_decimal(self):
        """Store and retrieve a Decimal"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": Decimal("1.1")})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["num"], Decimal("1.1"))

    def test_binary(self):
        """Store and retrieve a binary"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "data": Binary("abc")})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["data"].value, b"abc")

    def test_binary_bytes(self):
        """Store and retrieve bytes as a binary"""
        self.make_table()
        data = {"a": 1, "b": 2}
        self.dynamo.put_item("foobar", {"id": "a", "data": Binary(dumps(data))})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(loads(item["data"].value), data)

    def test_string_set(self):
        """Store and retrieve a string set"""
        self.make_table()
        item = {
            "id": "a",
            "datas": set(["a", "b"]),
        }
        self.dynamo.put_item("foobar", item)
        ret = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(ret, item)

    def test_number_set(self):
        """Store and retrieve a number set"""
        self.make_table()
        item = {
            "id": "a",
            "datas": set([1, 2, 3]),
        }
        self.dynamo.put_item("foobar", item)
        ret = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(ret, item)

    def test_binary_set(self):
        """Store and retrieve a binary set"""
        self.make_table()
        item = {
            "id": "a",
            "datas": set([Binary("a"), Binary("b")]),
        }
        self.dynamo.put_item("foobar", item)
        ret = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(ret, item)

    def test_binary_equal(self):
        """Binary should eq other Binaries and also raw bytestrings"""
        self.assertEqual(Binary("a"), Binary("a"))
        self.assertEqual(Binary("a"), b"a")
        self.assertFalse(Binary("a") != Binary("a"))

    def test_binary_repr(self):
        """Binary repr should wrap the contained value"""
        self.assertEqual(repr(Binary("a")), "Binary(%r)" % b"a")

    def test_binary_converts_unicode(self):
        """Binary will convert unicode to bytes"""
        b = Binary("a")
        self.assertTrue(isinstance(b.value, bytes))

    def test_binary_force_string(self):
        """Binary must wrap a string type"""
        with self.assertRaises(TypeError):
            Binary(2)  # type: ignore

    def test_bool(self):
        """Store and retrieve a boolean"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "abc", "b": True})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["b"], True)
        self.assertTrue(isinstance(item["b"], bool))

    def test_list(self):
        """Store and retrieve a list"""
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "abc", "l": ["a", 1, False]})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["l"], ["a", 1, False])

    def test_dict(self):
        """Store and retrieve a dict"""
        self.make_table()
        data = {
            "i": 1,
            "s": "abc",
            "n": None,
            "l": ["a", 1, True],
            "b": False,
        }
        self.dynamo.put_item("foobar", {"id": "abc", "d": data})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["d"], data)

    def test_nested_dict(self):
        """Store and retrieve a nested dict"""
        self.make_table()
        data = {
            "s": "abc",
            "d": {
                "i": 42,
            },
        }
        self.dynamo.put_item("foobar", {"id": "abc", "d": data})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["d"], data)

    def test_nested_list(self):
        """Store and retrieve a nested list"""
        self.make_table()
        data = [
            1,
            [
                True,
                None,
                "abc",
            ],
        ]
        self.dynamo.put_item("foobar", {"id": "abc", "l": data})
        item = list(self.dynamo.scan("foobar"))[0]
        self.assertEqual(item["l"], data)

    def test_unrecognized_type(self):
        """Dynamizer throws error on unrecognized type"""
        value = {
            "ASDF": "abc",
        }
        with self.assertRaises(TypeError):
            self.dynamo.dynamizer.decode(value)


class TestDynamizer(unittest.TestCase):

    """Tests for the Dynamizer"""

    def test_register_encoder(self):
        """Can register a custom encoder"""
        from datetime import datetime

        dynamizer = Dynamizer()
        dynamizer.register_encoder(datetime, lambda d, v: (STRING, v.isoformat()))
        now = datetime.utcnow()
        self.assertEqual(dynamizer.raw_encode(now), (STRING, now.isoformat()))

    def test_encoder_missing(self):
        """If no encoder is found, raise ValueError"""
        from datetime import datetime

        dynamizer = Dynamizer()
        with self.assertRaises(ValueError):
            dynamizer.encode(datetime.utcnow())


class TestResultModels(unittest.TestCase):

    """Tests for the model classes in results.py"""

    def test_add_dicts_base_case(self):
        """add_dict where one argument is None returns the other"""
        f = object()
        self.assertEqual(add_dicts(f, None), f)
        self.assertEqual(add_dicts(None, f), f)

    def test_add_dicts(self):
        """Merge two dicts of values together"""
        a = {
            "a": 1,
            "b": 2,
        }
        b = {
            "a": 3,
            "c": 4,
        }
        ret = add_dicts(a, b)
        self.assertEqual(
            ret,
            {
                "a": 4,
                "b": 2,
                "c": 4,
            },
        )

    def test_count_repr(self):
        """Count repr"""
        count = Count(0, 0)
        self.assertEqual(repr(count), "Count(0)")

    def test_count_addition(self):
        """Count addition"""
        count = Count(4, 2)
        self.assertEqual(count + 5, 9)

    def test_count_subtraction(self):
        """Count subtraction"""
        count = Count(4, 2)
        self.assertEqual(count - 2, 2)

    def test_count_multiplication(self):
        """Count multiplication"""
        count = Count(4, 2)
        self.assertEqual(2 * count, 8)

    def test_count_division(self):
        """Count division"""
        count = Count(4, 2)
        self.assertEqual(count / 2, 2)

    def test_count_add_none_capacity(self):
        """Count addition with one None consumed_capacity"""
        cap = Capacity(3, 0)
        count = Count(4, 2)
        count2 = Count(5, 3, cap)
        ret = count + count2
        self.assertEqual(ret, 9)
        self.assertEqual(ret.scanned_count, 5)
        self.assertEqual(ret.consumed_capacity, cap)

    def test_count_add_capacity(self):
        """Count addition with consumed_capacity"""
        count = Count(4, 2, Capacity(3, 0))
        count2 = Count(5, 3, Capacity(2, 0))
        ret = count + count2
        self.assertEqual(ret, 9)
        self.assertEqual(ret.scanned_count, 5)
        self.assertEqual(ret.consumed_capacity.read, 5)

    def test_capacity_math(self):
        """Capacity addition and equality"""
        cap = Capacity(2, 4)
        s = set([cap])
        self.assertIn(Capacity(2, 4), s)
        self.assertNotEqual(Capacity(1, 4), cap)
        self.assertEqual(Capacity(1, 1) + Capacity(2, 2), Capacity(3, 3))

    def test_capacity_format(self):
        """String formatting for Capacity"""
        c = Capacity(1, 3)
        self.assertEqual(str(c), "R:1.0 W:3.0")
        c = Capacity(0, 0)
        self.assertEqual(str(c), "0")

    def test_total_consumed_capacity(self):
        """ConsumedCapacity can parse results with only Total"""
        response = {
            "TableName": "foobar",
            "ReadCapacityUnits": 4,
            "WriteCapacityUnits": 5,
        }
        cap = ConsumedCapacity.from_response(response)
        self.assertEqual(cap.total, (4, 5))
        self.assertIsNone(cap.table_capacity)

    def test_consumed_capacity_equality(self):
        """ConsumedCapacity addition and equality"""
        cap = ConsumedCapacity(
            "foobar",
            Capacity(0, 10),
            Capacity(0, 2),
            {
                "l-index": Capacity(0, 4),
            },
            {
                "g-index": Capacity(0, 3),
            },
        )
        c2 = ConsumedCapacity(
            "foobar",
            Capacity(0, 10),
            Capacity(0, 2),
            {
                "l-index": Capacity(0, 4),
                "l-index2": Capacity(0, 7),
            },
        )

        self.assertNotEqual(cap, c2)
        c3 = ConsumedCapacity(
            "foobar",
            Capacity(0, 10),
            Capacity(0, 2),
            {
                "l-index": Capacity(0, 4),
            },
            {
                "g-index": Capacity(0, 3),
            },
        )
        self.assertIn(cap, set([c3]))
        combined = cap + c2
        self.assertEqual(
            cap + c2,
            ConsumedCapacity(
                "foobar",
                Capacity(0, 20),
                Capacity(0, 4),
                {
                    "l-index": Capacity(0, 8),
                    "l-index2": Capacity(0, 7),
                },
                {
                    "g-index": Capacity(0, 3),
                },
            ),
        )
        self.assertIn(str(Capacity(0, 3)), str(combined))

    def test_add_different_tables(self):
        """Cannot add ConsumedCapacity of two different tables"""
        c1 = ConsumedCapacity("foobar", Capacity(1, 28))
        c2 = ConsumedCapacity("boofar", Capacity(3, 0))
        with self.assertRaises(TypeError):
            c1 += c2

    def test_always_continue_query(self):
        """Regression test.
        If result has no items but does have LastEvaluatedKey, keep querying.
        """
        conn = MagicMock()
        conn.dynamizer.decode_keys.side_effect = lambda x: x
        items = ["a", "b"]
        results = [
            {"Items": [], "LastEvaluatedKey": {"foo": 1, "bar": 2}},
            {"Items": [], "LastEvaluatedKey": {"foo": 1, "bar": 2}},
            {"Items": items},
        ]
        conn.call.side_effect = lambda *_, **__: results.pop(0)
        rs = ResultSet(conn, Limit())
        results = list(rs)
        self.assertEqual(results, items)


class TestHooks(BaseSystemTest):

    """Tests for connection callback hooks"""

    def tearDown(self):
        super(TestHooks, self).tearDown()
        for hooks in self.dynamo._hooks.values():
            while hooks:
                hooks.pop()

    def test_precall(self):
        """precall hooks are called before an API call"""
        hook = MagicMock()
        self.dynamo.subscribe("precall", hook)

        def throw(**_):
            """Throw an exception to terminate the request"""
            raise Exception()

        with patch.object(self.dynamo, "client") as client:
            client.describe_table.side_effect = throw
            with self.assertRaises(Exception):
                self.dynamo.describe_table("foobar")
        hook.assert_called_with(self.dynamo, "describe_table", {"TableName": "foobar"})

    def test_postcall(self):
        """postcall hooks are called after API call"""
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)
        calls = []

        def hook(*args):
            """Log the call into a list"""
            calls.append(args)

        self.dynamo.subscribe("postcall", hook)
        self.dynamo.describe_table("foobar")
        self.assertEqual(len(calls), 1)
        args = calls[0]
        self.assertEqual(len(args), 4)
        conn, command, kwargs, response = args
        self.assertEqual(conn, self.dynamo)
        self.assertEqual(command, "describe_table")
        self.assertEqual(kwargs["TableName"], "foobar")
        self.assertEqual(response["Table"]["TableName"], "foobar")

    def test_capacity(self):
        """capacity hooks are called whenever response has ConsumedCapacity"""
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)
        hook = MagicMock()
        self.dynamo.subscribe("capacity", hook)
        with patch.object(self.dynamo, "client") as client:
            client.scan.return_value = {
                "Items": [],
                "ConsumedCapacity": {
                    "TableName": "foobar",
                    "ReadCapacityUnits": 4,
                },
            }
            rs = self.dynamo.scan("foobar")
            list(rs)
        cap = ConsumedCapacity("foobar", Capacity(4, 0))
        hook.assert_called_with(self.dynamo, "scan", ANY, ANY, cap)

    def test_subscribe(self):
        """Can subscribe and unsubscribe from hooks"""
        hook = lambda: None
        self.dynamo.subscribe("precall", hook)
        self.assertEqual(len(self.dynamo._hooks["precall"]), 1)
        self.dynamo.unsubscribe("precall", hook)
        self.assertEqual(len(self.dynamo._hooks["precall"]), 0)
