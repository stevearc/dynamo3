""" Wrappers for result objects and iterators """
from abc import ABC, abstractmethod, abstractproperty
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    List,
    Optional,
    Tuple,
    Union,
    ValuesView,
    overload,
)

from .constants import MAX_GET_BATCH, ReturnCapacityType
from .types import (
    Dynamizer,
    DynamoObject,
    EncodedDynamoObject,
    ExpressionAttributeNamesType,
)

if TYPE_CHECKING:
    from .connection import DynamoDBConnection


def add_dicts(d1, d2):
    """ Merge two dicts of addable values """
    if d1 is None:
        return d2
    if d2 is None:
        return d1
    keys = set(d1)
    keys.update(set(d2))
    ret = {}
    for key in keys:
        v1 = d1.get(key)
        v2 = d2.get(key)
        if v1 is None:
            ret[key] = v2
        elif v2 is None:
            ret[key] = v1
        else:
            ret[key] = v1 + v2
    return ret


class Count(int):

    """ Wrapper for response to query with Select=COUNT """

    count: int
    scanned_count: int
    consumed_capacity: Optional["Capacity"]

    def __new__(
        cls,
        count: int,
        scanned_count: int,
        consumed_capacity: Optional["Capacity"] = None,
    ) -> "Count":
        ret = super(Count, cls).__new__(cls, count)  # type: ignore
        ret.count = count
        ret.scanned_count = scanned_count
        ret.consumed_capacity = consumed_capacity
        return ret

    @classmethod
    def from_response(cls, response: Dict[str, Any]) -> "Count":
        """ Factory method """
        return cls(
            response["Count"],
            response["ScannedCount"],
            response.get("consumed_capacity"),
        )

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(other, Count):
            return self.count + other
        if self.consumed_capacity is None:
            capacity = other.consumed_capacity
        else:
            capacity = self.consumed_capacity + other.consumed_capacity
        return Count(
            self.count + other.count, self.scanned_count + other.scanned_count, capacity
        )

    def __radd__(self, other):
        return self.__add__(other)

    def __repr__(self):
        return "Count(%d)" % self


class Capacity(object):
    """ Wrapper for the capacity of a table or index """

    def __init__(self, read: float, write: float):
        self._read = read
        self._write = write

    @property
    def read(self) -> float:
        """ The read capacity """
        return self._read

    @property
    def write(self) -> float:
        """ The write capacity """
        return self._write

    @classmethod
    def from_response(
        cls, response: Dict[str, Any], is_read: Optional[bool]
    ) -> "Capacity":
        read = response.get("ReadCapacityUnits")
        if read is None:
            read = response["CapacityUnits"] if is_read else 0
        write = response.get("WriteCapacityUnits")
        if write is None:
            write = 0 if is_read else response["CapacityUnits"]
        return cls(read, write)

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        return key in ["read", "write"]

    def __hash__(self):
        return self._read + self._write

    def __eq__(self, other):
        if isinstance(other, tuple):
            return self.read == other[0] and self.write == other[1]
        return self.read == getattr(other, "read", None) and self.write == getattr(
            other, "write", None
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        if isinstance(other, tuple):
            return Capacity(self.read + other[0], self.write + other[1])
        return Capacity(self.read + other.read, self.write + other.write)

    def __radd__(self, other):
        return self.__add__(other)

    def __str__(self):
        pieces = []
        if self.read:
            pieces.append("R:{0:.1f}".format(self.read))
        if self.write:
            pieces.append("W:{0:.1f}".format(self.write))
        if not pieces:
            return "0"
        return " ".join(pieces)


class ConsumedCapacity(object):
    """ Record of the consumed capacity of a request """

    def __init__(
        self,
        tablename: str,
        total: Capacity,
        table_capacity: Optional[Capacity] = None,
        local_index_capacity: Optional[Dict[str, Capacity]] = None,
        global_index_capacity: Optional[Dict[str, Capacity]] = None,
    ):
        self.tablename = tablename
        self.total = total
        self.table_capacity = table_capacity
        self.local_index_capacity = local_index_capacity
        self.global_index_capacity = global_index_capacity

    @classmethod
    def build_indexes(
        cls, response: Dict[str, Dict[str, Any]], key: str, is_read: Optional[bool]
    ) -> Optional[Dict[str, Capacity]]:
        """ Construct index capacity map from a request fragment """
        if key not in response:
            return None
        indexes = {}
        for key, val in response[key].items():
            indexes[key] = Capacity.from_response(val, is_read)
        return indexes

    @classmethod
    def from_response(
        cls, response: Dict[str, Any], is_read: Optional[bool] = None
    ) -> "ConsumedCapacity":
        """ Factory method for ConsumedCapacity from a response object """
        kwargs = {
            "tablename": response["TableName"],
            "total": Capacity.from_response(response, is_read),
        }
        local = cls.build_indexes(response, "LocalSecondaryIndexes", is_read)
        kwargs["local_index_capacity"] = local
        gindex = cls.build_indexes(response, "GlobalSecondaryIndexes", is_read)
        kwargs["global_index_capacity"] = gindex
        if "Table" in response:
            kwargs["table_capacity"] = Capacity.from_response(
                response["Table"], is_read
            )
        return cls(**kwargs)

    def __hash__(self):
        return hash(self.tablename) + hash(self.total)

    def __eq__(self, other):
        properties = [
            "tablename",
            "total",
            "table_capacity",
            "local_index_capacity",
            "global_index_capacity",
        ]
        for prop in properties:
            if getattr(self, prop) != getattr(other, prop, None):
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __radd__(self, other):
        return self.__add__(other)

    def __add__(self, other):
        # Handle identity cases when added to empty values
        if other is None:
            return self
        if self.tablename != other.tablename:
            raise TypeError("Cannot add capacities from different tables")
        kwargs = {
            "total": self.total + other.total,
        }
        if self.table_capacity is not None:
            kwargs["table_capacity"] = self.table_capacity + other.table_capacity
        kwargs["local_index_capacity"] = add_dicts(
            self.local_index_capacity, other.local_index_capacity
        )
        kwargs["global_index_capacity"] = add_dicts(
            self.global_index_capacity, other.global_index_capacity
        )

        return ConsumedCapacity(self.tablename, **kwargs)

    def __str__(self):
        lines = []
        if self.table_capacity:
            lines.append("Table: %s" % self.table_capacity)
        if self.local_index_capacity:
            for name, cap in self.local_index_capacity.items():
                lines.append("Local index '%s': %s" % (name, cap))
        if self.global_index_capacity:
            for name, cap in self.global_index_capacity.items():
                lines.append("Global index '%s': %s" % (name, cap))
        lines.append("Total: %s" % self.total)
        return "\n".join(lines)


class PagedIterator(ABC):

    """ An iterator that iterates over paged results from Dynamo """

    def __init__(self):
        self.iterator = None

    @abstractproperty
    def can_fetch_more(self) -> bool:  # pragma: no cover
        """ Return True if more results can be fetched from the server """
        raise NotImplementedError

    @abstractmethod
    def _fetch(self) -> Iterator:  # pragma: no cover
        """ Fetch additional results from the server and return an iterator """
        raise NotImplementedError

    def __iter__(self):
        return self

    def __next__(self):
        if self.iterator is None:
            self.iterator = self._fetch()
        while True:
            try:
                return next(self.iterator)
            except StopIteration:
                if self.can_fetch_more:
                    self.iterator = self._fetch()
                else:
                    raise


class ResultSet(PagedIterator):

    """ Iterator that pages results from Dynamo """

    def __init__(
        self,
        connection: "DynamoDBConnection",
        limit: "Limit",
        *args: Any,
        **kwargs: Any
    ):
        super(ResultSet, self).__init__()
        self.connection = connection
        # The limit will be mutated, so copy it and leave the original intact
        self.limit = limit.copy()
        self.args = args
        self.kwargs = kwargs
        self.last_evaluated_key: Optional[dict] = None
        self.consumed_capacity: Optional[ConsumedCapacity] = None

    @property
    def can_fetch_more(self) -> bool:
        """ True if there are more results on the server """
        return self.last_evaluated_key is not None and not self.limit.complete

    def _fetch(self) -> Iterator:
        """ Fetch more results from Dynamo """
        self.limit.set_request_args(self.kwargs)
        data = self.connection.call(*self.args, **self.kwargs)
        self.limit.post_fetch(data)
        self.last_evaluated_key = data.get("LastEvaluatedKey")
        if self.last_evaluated_key is None:
            self.kwargs.pop("ExclusiveStartKey", None)
        else:
            self.kwargs["ExclusiveStartKey"] = self.last_evaluated_key
        if "consumed_capacity" in data:
            self.consumed_capacity += data["consumed_capacity"]
        for raw_item in data["Items"]:
            item = self.connection.dynamizer.decode_keys(raw_item)
            if self.limit.accept(item):
                yield item

    def __next__(self) -> DynamoObject:  # pylint: disable=W0235
        return super().__next__()


class GetResultSet(PagedIterator):

    """ Iterator that pages the results of a BatchGetItem """

    def __init__(
        self,
        connection: "DynamoDBConnection",
        keymap: Dict[str, Iterable[DynamoObject]],
        consistent: bool = False,
        attributes: Optional[str] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        return_capacity: Optional[ReturnCapacityType] = None,
    ):
        super(GetResultSet, self).__init__()
        self.connection = connection
        self.keymap: Dict[str, Iterator[DynamoObject]] = {
            t: iter(keys) for t, keys in keymap.items()
        }
        self.consistent = consistent
        self.attributes = attributes
        self.alias = alias
        self.return_capacity = return_capacity
        self._pending_keys: Dict[str, List[EncodedDynamoObject]] = {}
        self._attempt = 0
        self.consumed_capacity: Optional[Dict[str, ConsumedCapacity]] = None
        self._cached_dict: Optional[Dict[str, List[DynamoObject]]] = None
        self._started_iterator = False

    @property
    def can_fetch_more(self) -> bool:
        return bool(self.keymap) or bool(self._pending_keys)

    def build_kwargs(self):
        """ Construct the kwargs to pass to batch_get_item """
        num_pending = sum([len(v) for v in self._pending_keys.values()])
        if num_pending < MAX_GET_BATCH:
            tablenames_to_remove = []
            for tablename, key_iter in self.keymap.items():
                for key in key_iter:
                    pending_keys = self._pending_keys.setdefault(tablename, [])
                    pending_keys.append(self.connection.dynamizer.encode_keys(key))
                    num_pending += 1
                    if num_pending == MAX_GET_BATCH:
                        break
                else:
                    tablenames_to_remove.append(tablename)
                if num_pending == MAX_GET_BATCH:
                    break
            for tablename in tablenames_to_remove:
                self.keymap.pop(tablename, None)

        if not self._pending_keys:
            return None
        request_items = {}
        for tablename, keys in self._pending_keys.items():
            query: Dict[str, Any] = {"ConsistentRead": self.consistent}
            if self.attributes is not None:
                query["ProjectionExpression"] = self.attributes
            if self.alias:
                query["ExpressionAttributeNames"] = self.alias
            query["Keys"] = keys
            request_items[tablename] = query
        self._pending_keys = {}
        return {
            "RequestItems": request_items,
            "ReturnConsumedCapacity": self.return_capacity,
        }

    def _fetch(self) -> Iterator:
        """ Fetch a set of items from their keys """
        kwargs = self.build_kwargs()
        if kwargs is None:
            return iter([])
        data = self.connection.call("batch_get_item", **kwargs)
        if "UnprocessedKeys" in data:
            for tablename, items in data["UnprocessedKeys"].items():
                keys = self._pending_keys.setdefault(tablename, [])
                keys.extend(items["Keys"])
            # Getting UnprocessedKeys indicates that we are exceeding our
            # throughput. So sleep for a bit.
            self._attempt += 1
            self.connection.exponential_sleep(self._attempt)
        else:
            # No UnprocessedKeys means our request rate is fine, so we can
            # reset the attempt number.
            self._attempt = 0
        if "consumed_capacity" in data:
            self.consumed_capacity = self.consumed_capacity or {}
            for cap in data["consumed_capacity"]:
                self.consumed_capacity[
                    cap.tablename
                ] = cap + self.consumed_capacity.get(cap.tablename)
        for tablename, items in data["Responses"].items():
            for item in items:
                yield tablename, item

    def __getitem__(self, key: str) -> List[DynamoObject]:
        return self.asdict()[key]

    def items(self) -> ItemsView[str, List[DynamoObject]]:
        return self.asdict().items()

    def keys(self) -> KeysView[str]:
        return self.asdict().keys()

    def values(self) -> ValuesView[List[DynamoObject]]:
        return self.asdict().values()

    def __next__(self) -> Tuple[str, DynamoObject]:
        self._started_iterator = True
        tablename, result = super().__next__()
        return tablename, self.connection.dynamizer.decode_keys(result)

    def asdict(self) -> Dict[str, List[DynamoObject]]:
        if self._cached_dict is None:
            if self._started_iterator:
                raise ValueError(
                    "Cannot use asdict if also using GetResultSet as an iterator"
                )
            self._cached_dict = {}
            for tablename, item in self:
                items = self._cached_dict.setdefault(tablename, [])
                items.append(item)
        return self._cached_dict


class SingleTableGetResultSet(object):
    def __init__(self, result_set: GetResultSet):
        self.result_set = result_set

    @property
    def consumed_capacity(self) -> Optional[ConsumedCapacity]:
        """ Getter for consumed_capacity """
        cap_map = self.result_set.consumed_capacity
        if cap_map is None:
            return None
        return next(iter(cap_map.values()))

    def __iter__(self):
        return self

    def __next__(self) -> DynamoObject:
        return next(self.result_set)[1]


class TableResultSet(PagedIterator):

    """ Iterator that pages table names from ListTables """

    def __init__(self, connection: "DynamoDBConnection", limit: Optional[int] = None):
        super(TableResultSet, self).__init__()
        self.connection = connection
        self.limit = limit
        self.last_evaluated_table_name: Optional[str] = None

    @property
    def can_fetch_more(self) -> bool:
        if self.last_evaluated_table_name is None:
            return False
        return self.limit is None or self.limit > 0

    def _fetch(self) -> Iterator:
        kwargs: Dict[str, Any] = {}
        if self.limit is None:
            kwargs["Limit"] = 100
        else:
            kwargs["Limit"] = min(self.limit, 100)
        if self.last_evaluated_table_name is not None:
            kwargs["ExclusiveStartTableName"] = self.last_evaluated_table_name
        data = self.connection.call("list_tables", **kwargs)
        self.last_evaluated_table_name = data.get("LastEvaluatedTableName")
        tables = data["TableNames"]
        if self.limit is not None:
            self.limit -= len(tables)
        return iter(tables)

    def __next__(self) -> str:  # pylint: disable=W0235
        return super().__next__()


class Result(dict):

    """
    A wrapper for an item returned from Dynamo

    Attributes
    ----------
    consumed_capacity : :class:`~dynamo3.result.ConsumedCapacity`, optional
        Consumed capacity on the table
    exists : bool
        False if the result is empty (i.e. no result was returned from dynamo)

    """

    def __init__(self, dynamizer: Dynamizer, response: Dict[str, Any], item_key: str):
        super(Result, self).__init__()
        self.exists = item_key in response
        for k, v in response.get(item_key, {}).items():
            self[k] = dynamizer.decode(v)

        self.consumed_capacity: Optional[ConsumedCapacity] = response.get(
            "consumed_capacity"
        )

    def __repr__(self):
        return "Result({0})".format(super(Result, self).__repr__())


class Limit(object):

    """
    Class that defines query/scan limit behavior

    Parameters
    ----------
    scan_limit : int, optional
        The maximum number of items for DynamoDB to scan. This will not
        necessarily be the number of items returned.
    item_limit : int, optional
        The maximum number of items to return. Fetches will continue until this
        number is reached or there are no results left. See also: ``strict``
    min_scan_limit : int, optional
        This only matters when ``item_limit`` is set and ``scan_limit`` is not.
        After doing multiple fetches, the ``item_limit`` may drop to a low
        value. The ``item_limit`` will be passed up as the query ``Limit``, but
        if your ``item_limit`` is down to 1 you may want to fetch more than 1
        item at a time. ``min_scan_limit`` determines the minimum ``Limit`` to
        send up when ``scan_limit`` is None. (default 20)
    strict : bool, optional
        This modifies the behavior of ``item_limit``. If True, the query will
        never return more items than ``item_limit``. If False, the query will
        fetch until it hits the ``item_limit``, and then return the rest of the
        page as well. (default False)
    filter : callable, optional
        Function that takes a single item dict and returns a boolean. If True,
        the item will be counted towards the ``item_limit`` and returned from
        the iterator. If False, it will be skipped.

    """

    def __init__(
        self,
        scan_limit: Optional[int] = None,
        item_limit: Optional[int] = None,
        min_scan_limit: int = 20,
        strict: bool = False,
        filter: Callable[[DynamoObject], bool] = lambda x: True,
    ):
        self.scan_limit = scan_limit
        if item_limit is None:
            self.item_limit = scan_limit
        else:
            self.item_limit = item_limit
        self.min_scan_limit = min_scan_limit
        self.strict = strict
        self.filter = filter

    def copy(self) -> "Limit":
        """ Return a copy of the limit """
        return Limit(
            self.scan_limit,
            self.item_limit,
            self.min_scan_limit,
            self.strict,
            self.filter,
        )

    def set_request_args(self, args: Dict[str, Any]) -> None:
        """ Set the Limit parameter into the request args """
        if self.scan_limit is not None:
            args["Limit"] = self.scan_limit
        elif self.item_limit is not None:
            args["Limit"] = max(self.item_limit, self.min_scan_limit)
        else:
            args.pop("Limit", None)

    @property
    def complete(self) -> bool:
        """ Return True if the limit has been reached """
        if self.scan_limit is not None and self.scan_limit == 0:
            return True
        if self.item_limit is not None and self.item_limit == 0:
            return True
        return False

    def post_fetch(self, response: Dict[str, Any]) -> None:
        """ Called after a fetch. Updates the ScannedCount """
        if self.scan_limit is not None:
            self.scan_limit -= response["ScannedCount"]

    def accept(self, item: DynamoObject) -> bool:
        """ Apply the filter and item_limit, and return True to accept """
        accept = self.filter(item)
        if accept and self.item_limit is not None:
            if self.item_limit > 0:
                self.item_limit -= 1
            elif self.strict:
                return False
        return accept


class TransactionGet(object):
    def __init__(
        self,
        connection: "DynamoDBConnection",
        return_capacity: Optional[ReturnCapacityType] = None,
    ):
        self._connection = connection
        self._return_capacity = return_capacity
        self._cached_list: Optional[List[DynamoObject]] = None
        self.consumed_capacity: Optional[Dict[str, ConsumedCapacity]] = None
        self._items: List[
            Tuple[
                str,
                DynamoObject,
                Optional[Union[str, Iterable[str]]],
                Optional[ExpressionAttributeNamesType],
            ]
        ] = []

    def add_key(
        self,
        tablename: str,
        key: DynamoObject,
        attributes: Optional[Union[str, Iterable[str]]] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
    ) -> None:
        self._items.append((tablename, key, attributes, alias))

    def __iter__(self):
        return iter(self.aslist())

    @overload
    def __getitem__(self, index: int) -> DynamoObject:
        ...

    @overload
    def __getitem__(self, index: slice) -> List[DynamoObject]:
        ...

    def __getitem__(
        self, index: Union[int, slice]
    ) -> Union[DynamoObject, List[DynamoObject]]:
        return self.aslist()[index]

    def __len__(self):
        return len(self.aslist())

    def _fetch(self) -> List[DynamoObject]:
        if self._cached_list is not None or not self._items:
            return self._cached_list or []
        transact_items = []
        for (tablename, key, attributes, alias) in self._items:
            item = {
                "TableName": tablename,
                "Key": self._connection.dynamizer.encode_keys(key),
            }
            if attributes is not None:
                if not isinstance(attributes, str):
                    attributes = ", ".join(attributes)
                item["ProjectionExpression"] = attributes
            if alias is not None:
                item["ExpressionAttributeNames"] = alias
            transact_items.append({"Get": item})
        kwargs: Dict[str, Any] = {"TransactItems": transact_items}
        if self._return_capacity is not None:
            kwargs["ReturnConsumedCapacity"] = self._return_capacity
        response = self._connection.call("transact_get_items", **kwargs)
        if "consumed_capacity" in response:
            self.consumed_capacity = self.consumed_capacity or {}
            for cap in response["consumed_capacity"]:
                self.consumed_capacity[
                    cap.tablename
                ] = cap + self.consumed_capacity.get(cap.tablename)
        decoded = []
        for response_item in response["Responses"]:
            decoded.append(
                self._connection.dynamizer.decode_keys(response_item["Item"])
            )
        return decoded

    def aslist(self) -> List[DynamoObject]:
        if self._cached_list is None:
            self._cached_list = self._fetch()
        return self._cached_list
