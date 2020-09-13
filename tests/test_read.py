""" Test the read functions of Dynamo """

from mock import MagicMock

from dynamo3 import DynamoKey, GlobalIndex, LocalIndex
from dynamo3.constants import NUMBER, STRING, TOTAL
from dynamo3.result import (
    Capacity,
    ConsumedCapacity,
    GetResultSet,
    Limit,
    Result,
    SingleTableGetResultSet,
)

from . import BaseSystemTest


class TestQuery2(BaseSystemTest):

    """ Tests for the newer query API """

    def make_table(self):
        """ Convenience method for making a table """
        hash_key = DynamoKey("id")
        range_key = DynamoKey("num", data_type=NUMBER)
        self.dynamo.create_table("foobar", hash_key=hash_key, range_key=range_key)

    def test_hash(self):
        """ Can query on the hash key """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        self.dynamo.put_item("foobar", {"id": "a"})
        results = self.dynamo.query("foobar", "id = :id", id="a")
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_local_index(self):
        """ Can query on a local index """
        hash_key = DynamoKey("id", data_type=STRING)
        range_key = DynamoKey("num", data_type=NUMBER)
        index_field = DynamoKey("name")
        index = LocalIndex.keys("name-index", index_field)
        self.dynamo.create_table("foobar", hash_key, range_key, indexes=[index])
        item = {
            "id": "a",
            "num": 1,
            "name": "baz",
        }
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query(
            "foobar",
            "id = :id and #name = :name",
            alias={"#name": "name"},
            index="name-index",
            id="a",
            name="baz",
        )
        self.assertCountEqual(list(ret), [item])

    def test_global_index(self):
        """ Can query on a global index """
        hash_key = DynamoKey("id", data_type=STRING)
        index_field = DynamoKey("name")
        index = GlobalIndex.all("name-index", index_field)
        self.dynamo.create_table("foobar", hash_key, global_indexes=[index])
        item = {
            "id": "a",
            "name": "baz",
        }
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query(
            "foobar",
            "#name = :name",
            alias={"#name": "name"},
            index="name-index",
            filter="id = :id",
            id="a",
            name="baz",
        )
        self.assertCountEqual(list(ret), [item])

    def test_attributes(self):
        """ Can select only certain attributes """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        item = {
            "id": "a",
            "foo": "bar",
        }
        self.dynamo.put_item("foobar", item)
        results = self.dynamo.query("foobar", "id = :id", attributes="id", id="a")
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_attributes_list(self):
        """ Can select only certain attributes via list """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        item = {
            "id": "a",
            "foo": "bar",
        }
        self.dynamo.put_item("foobar", item)
        results = self.dynamo.query("foobar", "id = :id", attributes=["id"], id="a")
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_order_desc(self):
        """ Can sort the results in descending order """
        self.make_table()
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(3):
                batch.put({"id": "a", "num": i})
        ret = self.dynamo.query(
            "foobar", "id = :id", attributes="num", desc=True, id="a"
        )
        self.assertEqual(list(ret), [{"num": i} for i in range(2, -1, -1)])

    def test_limit(self):
        """ Can limit the number of query results """
        self.make_table()
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(3):
                batch.put({"id": "a", "num": i})
        ret = self.dynamo.query("foobar", "id = :id", limit=1, id="a")
        self.assertEqual(len(list(ret)), 1)

    def test_count(self):
        """ Can count items instead of returning the actual items """
        self.make_table()
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(3):
                batch.put({"id": "a", "num": i})
        ret = self.dynamo.query("foobar", "id = :id", select="COUNT", id="a")
        self.assertEqual(ret, 3)
        self.assertEqual(ret.scanned_count, 3)

    def test_capacity(self):
        """ Can return consumed capacity """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        ret = self.dynamo.query("foobar", "id = :id", return_capacity=TOTAL, id="a")
        list(ret)
        assert ret.consumed_capacity is not None
        self.assertTrue(isinstance(ret.consumed_capacity.total, Capacity))

    def test_eq(self):
        """ Can query with EQ constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", item)
        self.dynamo.put_item("foobar", {"id": "a", "num": 2})
        ret = self.dynamo.query("foobar", "id = :id and num = :num", id="a", num=1)
        self.assertCountEqual(list(ret), [item])

    def test_le(self):
        """ Can query with <= constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", item)
        self.dynamo.put_item("foobar", {"id": "a", "num": 2})
        ret = self.dynamo.query("foobar", "id = :id and num <= :num", id="a", num=1)
        self.assertCountEqual(list(ret), [item])

    def test_lt(self):
        """ Can query with < constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", item)
        self.dynamo.put_item("foobar", {"id": "a", "num": 2})
        ret = self.dynamo.query("foobar", "id = :id and num < :num", id="a", num=2)
        self.assertCountEqual(list(ret), [item])

    def test_ge(self):
        """ Can query with >= constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query("foobar", "id = :id and num >= :num", id="a", num=2)
        self.assertCountEqual(list(ret), [item])

    def test_gt(self):
        """ Can query with > constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query("foobar", "id = :id and num > :num", id="a", num=1)
        self.assertCountEqual(list(ret), [item])

    def test_beginswith(self):
        """ Can query with 'begins with' constraint """
        hash_key = DynamoKey("id")
        range_key = DynamoKey("name")
        self.dynamo.create_table("foobar", hash_key=hash_key, range_key=range_key)
        item = {"id": "a", "name": "David"}
        self.dynamo.put_item("foobar", {"id": "a", "name": "Steven"})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query(
            "foobar",
            "id = :id and begins_with(#name, :name)",
            alias={"#name": "name"},
            id="a",
            name="D",
        )
        self.assertCountEqual(list(ret), [item])

    def test_between(self):
        """ Can query with 'between' constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.query(
            "foobar", "id = :id and num between :low and :high", id="a", low=2, high=10
        )
        self.assertCountEqual(list(ret), [item])

    def test_no_kwargs(self):
        """ Expression values can be passed in as a dict """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        self.dynamo.put_item("foobar", {"id": "a"})
        results = self.dynamo.query("foobar", "id = :id", expr_values={":id": "a"})
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_filter(self):
        """ Query can filter returned results """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1, "a": "a"})
        self.dynamo.put_item("foobar", {"id": "a", "num": 2, "b": "b"})
        results = self.dynamo.query(
            "foobar", "id = :id", filter="a = :a", id="a", a="a"
        )
        self.assertCountEqual(list(results), [{"id": "a", "num": 1, "a": "a"}])

    def test_filter_and(self):
        """ Can 'and' the filter arguments """
        self.make_table()
        a = {"id": "a", "num": 1, "a": "a", "b": "a"}
        self.dynamo.put_item("foobar", a)
        b = {"id": "a", "num": 2, "a": "a", "b": "b"}
        self.dynamo.put_item("foobar", b)
        results = self.dynamo.query(
            "foobar", "id = :id", filter="a = :a and b = :b", id="a", a="a", b="a"
        )
        self.assertCountEqual(list(results), [a])

    def test_filter_or(self):
        """ Can 'or' the filter arguments """
        self.make_table()
        a = {"id": "a", "num": 1, "a": "a", "b": "a"}
        self.dynamo.put_item("foobar", a)
        b = {"id": "a", "num": 2, "a": "a", "b": "b"}
        self.dynamo.put_item("foobar", b)
        results = self.dynamo.query(
            "foobar", "id = :id", filter="a = :a or b = :b", id="a", a="a", b="a"
        )
        self.assertCountEqual(list(results), [a, b])

    def test_exclusive_start_key(self):
        """ Provide an ExclusiveStartKey to resume a query """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        self.dynamo.put_item("foobar", {"id": "a", "num": 2})
        self.dynamo.put_item("foobar", {"id": "a", "num": 3})
        results = self.dynamo.query(
            "foobar", "id = :id", exclusive_start_key={"id": "a", "num": 2}, id="a"
        )
        self.assertEqual(list(results), [{"id": "a", "num": 3}])

    def test_exclusive_start_key_format(self):
        """ Provide an ExclusiveStartKey already in DynamoDB format """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "num": 1})
        self.dynamo.put_item("foobar", {"id": "a", "num": 2})
        self.dynamo.put_item("foobar", {"id": "a", "num": 3})
        key = {
            "id": {
                "S": "a",
            },
            "num": {
                "N": "2",
            },
        }
        results = self.dynamo.query(
            "foobar", "id = :id", exclusive_start_key=key, id="a"
        )
        self.assertEqual(list(results), [{"id": "a", "num": 3}])


class TestScan2(BaseSystemTest):

    """ Tests for scan api """

    def make_table(self):
        """ Convenience method for making a table """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)

    def test_attributes(self):
        """ Can select only certain attributes """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        item = {
            "id": "a",
            "foo": "bar",
        }
        self.dynamo.put_item("foobar", item)
        results = self.dynamo.scan("foobar", attributes="id")
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_attributes_list(self):
        """ Can select only certain attributes specified by a list """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key)
        item = {
            "id": "a",
            "foo": "bar",
        }
        self.dynamo.put_item("foobar", item)
        results = self.dynamo.scan("foobar", attributes=["id"])
        self.assertCountEqual(list(results), [{"id": "a"}])

    def test_limit(self):
        """ Can limit the number of scan results """
        self.make_table()
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(3):
                batch.put({"id": str(i)})
        ret = self.dynamo.scan("foobar", limit=1)
        self.assertEqual(len(list(ret)), 1)

    def test_count(self):
        """ Can count items instead of returning the actual items """
        self.make_table()
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(3):
                batch.put({"id": str(i)})
        ret = self.dynamo.scan("foobar", select="COUNT")
        self.assertEqual(ret.count, 3)
        self.assertEqual(ret, 3)
        self.assertEqual(ret.scanned_count, 3)

    def test_count_page(self):
        """ Count queries should page the results """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        self.dynamo.put_item("foobar", {"id": "b"})

        def key_hook(connection, command, kwargs, data):
            """ Inject a fake LastEvaluatedKey and unsubscribe """
            data["LastEvaluatedKey"] = {"id": {"S": "b"}}
            self.dynamo.unsubscribe("postcall", key_hook)

        self.dynamo.subscribe("postcall", key_hook)
        ret = self.dynamo.scan("foobar", select="COUNT")
        self.assertEqual(ret, 3)

    def test_capacity(self):
        """ Can return consumed capacity """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        ret = self.dynamo.scan("foobar", return_capacity=TOTAL)
        list(ret)
        assert ret.consumed_capacity is not None
        self.assertTrue(isinstance(ret.consumed_capacity.total, Capacity))

    def test_eq(self):
        """ Can scan with EQ constraint """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        self.dynamo.put_item("foobar", {"id": "b"})
        ret = self.dynamo.scan("foobar", filter="id = :id", id="a")
        self.assertCountEqual(list(ret), [{"id": "a"}])

    def test_expr_values(self):
        """ Can pass in ExpressionAttributeValues direcly """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        self.dynamo.put_item("foobar", {"id": "b"})
        ret = self.dynamo.scan("foobar", filter="id = :id", expr_values={":id": "a"})
        self.assertCountEqual(list(ret), [{"id": "a"}])

    def test_ne(self):
        """ Can scan with NE constraint """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        self.dynamo.put_item("foobar", {"id": "b"})
        ret = self.dynamo.scan("foobar", filter="id <> :id", id="b")
        self.assertCountEqual(list(ret), [{"id": "a"}])

    def test_le(self):
        """ Can scan with <= constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", item)
        self.dynamo.put_item("foobar", {"id": "b", "num": 2})
        ret = self.dynamo.scan("foobar", filter="num <= :num", num=1)
        self.assertCountEqual(list(ret), [item])

    def test_lt(self):
        """ Can scan with < constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", item)
        self.dynamo.put_item("foobar", {"id": "b", "num": 2})
        ret = list(self.dynamo.scan("foobar", filter="num < :num", num=2))
        self.assertCountEqual(ret, [item])

    def test_ge(self):
        """ Can scan with >= constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "b", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan("foobar", filter="num >= :num", num=2)
        self.assertCountEqual(list(ret), [item])

    def test_gt(self):
        """ Can scan with > constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "b", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan("foobar", filter="num > :num", num=1)
        self.assertCountEqual(list(ret), [item])

    def test_beginswith(self):
        """ Can scan with 'begins with' constraint """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)
        item = {"id": "a", "name": "David"}
        self.dynamo.put_item("foobar", {"id": "b", "name": "Steven"})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan(
            "foobar",
            filter="begins_with(#name, :name)",
            alias={"#name": "name"},
            name="D",
        )
        self.assertCountEqual(list(ret), [item])

    def test_between(self):
        """ Can scan with 'between' constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "b", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan(
            "foobar", filter="num between :low and :high", low=2, high=10
        )
        self.assertCountEqual(list(ret), [item])

    def test_in(self):
        """ Can scan with 'in' constraint """
        self.make_table()
        item = {"id": "a", "num": 2}
        self.dynamo.put_item("foobar", {"id": "b", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan(
            "foobar", filter="num in (:v1, :v2, :v3, :v4)", v1=2, v2=3, v3=4, v4=4
        )
        self.assertCountEqual(list(ret), [item])

    def test_contains(self):
        """ Can scan with 'contains' constraint """
        self.make_table()
        item = {"id": "a", "nums": set([1, 2, 3])}
        self.dynamo.put_item("foobar", {"id": "b", "nums": set([4, 5, 6])})
        self.dynamo.put_item("foobar", item)
        ret = list(self.dynamo.scan("foobar", filter="contains(nums, :num)", num=2))
        self.assertCountEqual(ret, [item])

    def test_ncontains(self):
        """ Can scan with 'not contains' constraint """
        self.make_table()
        item = {"id": "a", "nums": set([1, 2, 3])}
        self.dynamo.put_item("foobar", {"id": "b", "nums": set([4, 5, 6])})
        self.dynamo.put_item("foobar", item)
        ret = list(self.dynamo.scan("foobar", filter="not contains(nums, :num)", num=4))
        self.assertCountEqual(ret, [item])

    def test_is_null(self):
        """ Can scan with 'is null' constraint """
        self.make_table()
        item = {"id": "a"}
        self.dynamo.put_item("foobar", {"id": "b", "num": 1})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan("foobar", filter="not attribute_exists(num)")
        self.assertCountEqual(list(ret), [item])

    def test_is_not_null(self):
        """ Can scan with 'is not null' constraint """
        self.make_table()
        item = {"id": "a", "num": 1}
        self.dynamo.put_item("foobar", {"id": "b"})
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.scan("foobar", filter="attribute_exists(num)")
        self.assertCountEqual(list(ret), [item])

    def test_filter_and(self):
        """ Multiple filter args are ANDed together """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "a": "a", "b": "a"})
        self.dynamo.put_item("foobar", {"id": "b", "a": "a", "b": "b"})
        ret = self.dynamo.scan("foobar", filter="a = :a and b = :b", a="a", b="a")
        self.assertCountEqual(list(ret), [{"id": "a", "a": "a", "b": "a"}])

    def test_filter_or(self):
        """ Can 'or' the filter arguments """
        self.make_table()
        a = {"id": "a", "a": "a", "b": "a"}
        self.dynamo.put_item("foobar", a)
        b = {"id": "b", "a": "a", "b": "b"}
        self.dynamo.put_item("foobar", b)
        ret = self.dynamo.scan("foobar", filter="a = :a or b = :b", a="a", b="a")
        self.assertCountEqual(list(ret), [a, b])

    def test_scan_index(self):
        """ Can scan a global index """
        hash_key = DynamoKey("id", data_type=STRING)
        index_field = DynamoKey("name")
        index = GlobalIndex.all("name-index", index_field)
        self.dynamo.create_table("foobar", hash_key, global_indexes=[index])
        item = {
            "id": "a",
            "name": "baz",
        }
        self.dynamo.put_item("foobar", item)
        item2 = {
            "id": "b",
        }
        self.dynamo.put_item("foobar", item2)
        ret = self.dynamo.scan("foobar", index="name-index")
        self.assertCountEqual(list(ret), [item])

    def test_parallel_scan(self):
        """ Can scan a table in segments """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        self.dynamo.put_item("foobar", {"id": "b"})
        self.dynamo.put_item("foobar", {"id": "c"})
        self.dynamo.put_item("foobar", {"id": "d"})
        ret1 = self.dynamo.scan("foobar", segment=0, total_segments=2)
        ret2 = self.dynamo.scan("foobar", segment=1, total_segments=2)
        self.assertCountEqual(
            list(ret1) + list(ret2),
            [{"id": "a"}, {"id": "b"}, {"id": "c"}, {"id": "d"}],
        )

    def test_exclusive_start_key(self):
        """ Provide an ExclusiveStartKey to resume a scan """
        self.make_table()
        items = [{"id": x} for x in ["a", "b", "c"]]
        a, b, c = items
        self.dynamo.put_item("foobar", a)
        self.dynamo.put_item("foobar", b)
        self.dynamo.put_item("foobar", c)
        first_item = list(self.dynamo.scan("foobar", limit=1))[0]
        items.remove(first_item)
        results = self.dynamo.scan("foobar", exclusive_start_key=first_item)
        self.assertCountEqual(list(results), items)


class TestBatchGet(BaseSystemTest):

    """ Tests for the BatchGetItem call """

    def make_table(self):
        """ Convenience method for making a table """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)

    def test_get_items(self):
        """ Can get multiple items """
        self.make_table()
        keys = [{"id": "a"}, {"id": "b"}]
        self.dynamo.put_item("foobar", keys[0])
        self.dynamo.put_item("foobar", keys[1])
        ret = list(self.dynamo.batch_get("foobar", keys))
        self.assertCountEqual(ret, keys)

    def test_get_many(self):
        """ Can get many items via paging """
        self.make_table()
        keys = [{"id": str(i)} for i in range(150)]
        with self.dynamo.batch_write("foobar") as batch:
            for key in keys:
                batch.put(key)
        ret = list(self.dynamo.batch_get("foobar", keys))
        self.assertCountEqual(ret, keys)

    def test_get_multiple_tables(self):
        """ Can get items from multiple tables """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foo", hash_key=hash_key)
        self.dynamo.create_table("bar", hash_key=hash_key)
        keys = [{"id": str(i)} for i in range(150)]
        with self.dynamo.batch_write("foo") as batch:
            for key in keys:
                batch.put(key)
        with self.dynamo.batch_write("bar") as batch:
            for key in keys:
                batch.put(key)
        result = self.dynamo.batch_get({"foo": keys, "bar": keys})
        self.assertCountEqual(result["foo"], keys)
        self.assertCountEqual(result["bar"], keys)

    def test_attributes(self):
        """ Can limit fetch to specific attributes """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "foo": "bar"})
        ret = list(self.dynamo.batch_get("foobar", [{"id": "a"}], attributes=["id"]))
        self.assertCountEqual(ret, [{"id": "a"}])

    def test_alias_attributes(self):
        """ Can alias the names of certain attributes """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a", "foo": "bar"})
        ret = self.dynamo.batch_get(
            "foobar", [{"id": "a"}], attributes=["#f"], alias={"#f": "id"}
        )
        self.assertCountEqual(list(ret), [{"id": "a"}])

    def test_handle_unprocessed(self):
        """ Batch get retries unprocessed keys """
        conn = MagicMock()
        # Pass responses through dynamizer unchanged
        conn.dynamizer.decode_keys.side_effect = lambda x: x
        key1, key2 = "key1", "key2"
        unprocessed = [[key1], [key2], []]
        val1, val2, val3 = "val1", "val2", "val3"
        values = [[val1], [val2], [val3]]
        conn.call.side_effect = lambda *_, **__: {
            "UnprocessedKeys": {
                "foo": {
                    "Keys": unprocessed.pop(0),
                },
            },
            "Responses": {
                "foo": values.pop(0),
            },
        }
        grs = GetResultSet(conn, {"foo": [{"id": "a"}]})
        rs = SingleTableGetResultSet(grs)
        results = list(rs)
        self.assertEqual(results, [val1, val2, val3])

    def test_capacity(self):
        """ Can return consumed capacity """
        conn = MagicMock()
        response_cap = {
            "TableName": "foobar",
            "CapacityUnits": 6,
            "Table": {
                "CapacityUnits": 1,
            },
            "LocalSecondaryIndexes": {
                "l-index": {
                    "CapacityUnits": 2,
                },
            },
            "GlobalSecondaryIndexes": {
                "g-index": {
                    "CapacityUnits": 3,
                },
            },
        }
        response = {
            "Responses": {
                "foo": [],
            },
            "ConsumedCapacity": [response_cap],
        }
        capacity = ConsumedCapacity.from_response(response_cap, True)
        response["consumed_capacity"] = [capacity]
        conn.call.return_value = response
        rs = GetResultSet(conn, {"foo": [{"id": "a"}]})
        list(rs)
        assert rs.consumed_capacity is not None
        assert rs.consumed_capacity.table_capacity is not None
        assert rs.consumed_capacity.local_index_capacity is not None
        assert rs.consumed_capacity.global_index_capacity is not None
        self.assertEqual(rs.consumed_capacity.total.read, 6)
        self.assertEqual(rs.consumed_capacity.table_capacity.read, 1)
        self.assertEqual(rs.consumed_capacity.local_index_capacity["l-index"].read, 2)
        self.assertEqual(rs.consumed_capacity.global_index_capacity["g-index"].read, 3)
        self.assertEqual(rs.consumed_capacity, capacity)


class TestGetItem2(BaseSystemTest):

    """ Tests for new GetItem API """

    def make_table(self):
        """ Convenience method for making a table """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)

    def test_get(self):
        """ Can fetch an item by the primary key """
        self.make_table()
        item = {"id": "a", "foo": "bar"}
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.get_item("foobar", {"id": "a"})
        self.assertTrue(ret.exists)
        self.assertEqual(ret, item)

    def test_get_missing(self):
        """ Fetching a missing item returns None """
        self.make_table()
        ret = self.dynamo.get_item("foobar", {"id": "a"})
        self.assertFalse(ret.exists)

    def test_attribute(self):
        """ Can fetch only certain attributes """
        self.make_table()
        item = {"id": "a", "foo": "bar"}
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.get_item("foobar", {"id": "a"}, attributes="id")
        self.assertEqual(ret, {"id": "a"})

    def test_attribute_alias(self):
        """ GetItem with ExpressionAttributeNames """
        self.make_table()
        item = {"id": "a", "foo": "bar"}
        self.dynamo.put_item("foobar", item)
        ret = self.dynamo.get_item(
            "foobar", {"id": "a"}, attributes=["#i"], alias={"#i": "id"}
        )
        self.assertEqual(ret, {"id": "a"})

    def test_capacity(self):
        """ Can return the consumed capacity as well """
        self.make_table()
        self.dynamo.put_item("foobar", {"id": "a"})
        ret = self.dynamo.get_item("foobar", {"id": "a"}, return_capacity=TOTAL)
        assert ret.consumed_capacity is not None
        self.assertTrue(isinstance(ret.consumed_capacity.total, Capacity))

    def test_result_repr(self):
        """ Result repr should not be the same as a dict """
        d = {"a": "b"}
        response = {"Item": self.dynamo.dynamizer.encode_keys(d)}
        result = Result(self.dynamo.dynamizer, response, "Item")
        self.assertNotEqual(repr(result), repr(d))


class TestLimit(BaseSystemTest):
    """ Tests for the Limit class """

    def make_table(self, items=10):
        """ Convenience method for making a table """
        hash_key = DynamoKey("id")
        self.dynamo.create_table("foobar", hash_key=hash_key)
        with self.dynamo.batch_write("foobar") as batch:
            for i in range(items):
                batch.put({"id": str(i), "num": i})

    def test_scan_limit(self):
        """ Limit can specify a max number of items to scan """
        self.make_table()
        ret = self.dynamo.scan("foobar", limit=Limit(5))
        self.assertEqual(len(list(ret)), 5)

    def test_item_limit(self):
        """ An item_limit will fetch until reached or exceeded """
        self.make_table()
        limit = Limit(item_limit=5)
        ret = self.dynamo.scan("foobar", limit=limit)
        self.assertEqual(len(list(ret)), 10)

    def test_strict(self):
        """ Strict=True will end the list exactly at the item_limit """
        self.make_table()
        ret = self.dynamo.scan("foobar", limit=Limit(item_limit=5, strict=True))
        self.assertEqual(len(list(ret)), 5)

    def test_filter(self):
        """ Limit can post-process to filter items """
        self.make_table()
        limit = Limit(filter=lambda x: x["num"] > 5)
        ret = self.dynamo.scan("foobar", limit=limit)
        self.assertEqual(len(list(ret)), 4)
