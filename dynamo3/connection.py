""" Connection class for DynamoDB """
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, overload

import botocore.session
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from typing_extensions import Literal

from .batch import BatchWriter
from .constants import (
    COUNT,
    INDEXES,
    NONE,
    READ_COMMANDS,
    NonCountSelectType,
    ReturnCapacityType,
    ReturnItemCollectionMetricsType,
    SelectType,
)
from .exception import DynamoDBError, ThroughputException, translate_exception
from .fields import DynamoKey, GlobalIndex, IndexUpdate, LocalIndex, Table, Throughput
from .rate import RateLimit
from .result import (
    ConsumedCapacity,
    Count,
    GetResultSet,
    Limit,
    Result,
    ResultSet,
    TableResultSet,
)
from .types import Dynamizer, is_null


def build_expected(dynamizer: Dynamizer, expected: Dict[str, Any]) -> Dict[str, Any]:
    """ Build the Expected parameters from a dict """
    ret = {}
    for k, v in expected.items():
        if is_null(v):
            ret[k] = {
                "Exists": False,
            }
        else:
            ret[k] = {
                "Exists": True,
                "Value": dynamizer.encode(v),
            }
    return ret


ExpressionValueType = Any
ExpressionValuesType = Dict[str, ExpressionValueType]
ExpressionAttributeNamesType = Dict[str, str]
DynamoObject = Dict[str, Any]
EncodedDynamoObject = Dict[str, Any]
HookType = Literal[Literal["precall"], Literal["postcall"], Literal["capacity"]]


def build_expression_values(
    dynamizer: Dynamizer,
    expr_values: Optional[ExpressionValuesType],
    kwargs: ExpressionValueType,
) -> Optional[EncodedDynamoObject]:
    """ Build ExpresionAttributeValues from a value or kwargs """
    if expr_values:
        values = expr_values
        return dynamizer.encode_keys(values)
    elif kwargs:
        values = dict(((":" + k, v) for k, v in kwargs.items()))
        return dynamizer.encode_keys(values)
    return None


class DynamoDBConnection(object):

    """
    Connection to DynamoDB.

    You should generally call :meth:`~.connect` instead of the constructor.

    Parameters
    ----------
    client : :class:`~botocore.client.BaseClient`, optional
        The botocore client that will be used for requests
    dynamizer : :class:`~dynamo3.types.Dynamizer`, optional
        The Dynamizer object to use for encoding/decoding values

    Attributes
    ----------
    request_retries : int
        Number of times to retry an API call if the throughput is exceeded
        (default 10)
    default_return_capacity : bool
        If true, all relevant calls will default to fetching the
        ConsumedCapacity

    """

    def __init__(
        self,
        client: BaseClient,
        dynamizer: Dynamizer = Dynamizer(),
    ):
        self.client = client
        self.dynamizer = dynamizer
        self.request_retries = 10
        self.default_return_capacity = False
        self._hooks: Dict[HookType, List[Callable]] = {
            "precall": [],
            "postcall": [],
            "capacity": [],
        }
        self.rate_limiters: List[RateLimit] = []

    @property
    def host(self) -> str:
        """ The address of the endpoint """
        return self.client.meta.endpoint_url

    @property
    def region(self) -> str:
        """ The name of the current connected region """
        return self.client.meta.region_name

    @classmethod
    def connect(
        cls,
        region: str,
        session: Optional[botocore.session.Session] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        host: Optional[str] = None,
        port: int = 80,
        is_secure: bool = True,
        dynamizer: Dynamizer = Dynamizer(),
    ) -> "DynamoDBConnection":
        """
        Connect to an AWS region.

        Parameters
        ----------
        region : str
            Name of an AWS region
        session : :class:`~botocore.session.Session`, optional
            The Session object to use for the connection
        access_key : str, optional
            If session is None, set this access key when creating the session
        secret_key : str, optional
            If session is None, set this secret key when creating the session
        host : str, optional
            Address of the host. Use this to connect to a local instance.
        port : int, optional
            Connect to the host on this port (default 80)
        is_secure : bool, optional
            Enforce https connection (default True)
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        if session is None:
            session = botocore.session.get_session()
            if access_key is not None:
                session.set_credentials(access_key, secret_key)
        url = None
        if host is not None:
            protocol = "https" if is_secure else "http"
            url = "%s://%s:%d" % (protocol, host, port)
        client = session.create_client(
            "dynamodb", region, endpoint_url=url, use_ssl=is_secure
        )
        return cls(client, dynamizer)

    def call(self, command: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Make a request to DynamoDB using the raw botocore API

        Parameters
        ----------
        command : str
            The name of the Dynamo command to execute
        **kwargs : dict
            The parameters to pass up in the request

        Raises
        ------
        exc : :class:`~.DynamoDBError`

        Returns
        -------
        data : dict

        """
        for hook in self._hooks["precall"]:
            hook(self, command, kwargs)
        op = getattr(self.client, command)
        attempt = 0
        while True:
            try:
                data = op(**kwargs)
                break
            except ClientError as e:
                exc = translate_exception(e, kwargs)
                attempt += 1
                if isinstance(exc, ThroughputException):
                    if attempt > self.request_retries:
                        exc.re_raise()
                    self.exponential_sleep(attempt)
                else:
                    exc.re_raise()
        for hook in self._hooks["postcall"]:
            hook(self, command, kwargs, data)
        if "ConsumedCapacity" in data:
            is_read = command in READ_COMMANDS
            consumed = data["ConsumedCapacity"]
            if isinstance(consumed, list):
                data["consumed_capacity"] = [
                    ConsumedCapacity.from_response(cap, is_read) for cap in consumed
                ]
            else:
                capacity = ConsumedCapacity.from_response(consumed, is_read)
                data["consumed_capacity"] = capacity
        if "consumed_capacity" in data:
            if isinstance(data["consumed_capacity"], list):
                all_caps = data["consumed_capacity"]
            else:
                all_caps = [data["consumed_capacity"]]
            for hook in self._hooks["capacity"]:
                for cap in all_caps:
                    hook(self, command, kwargs, data, cap)
        return data

    def exponential_sleep(self, attempt: int):
        """ Sleep with exponential backoff """
        if attempt > 1:
            time.sleep(0.1 * 2 ** attempt)

    def subscribe(self, event: HookType, hook: Callable):
        """
        Subscribe a callback to an event

        Parameters
        ----------
        event : str
            Available events are 'precall', 'postcall', and 'capacity'.
            precall is called with: (connection, command, query_kwargs)
            postcall is called with: (connection, command, query_kwargs, response)
            capacity is called with: (connection, command, query_kwargs, response, capacity)
        hook : callable

        """
        if hook not in self._hooks[event]:
            self._hooks[event].append(hook)

    def unsubscribe(self, event: HookType, hook: Callable):
        """ Unsubscribe a hook from an event """
        if hook in self._hooks[event]:
            self._hooks[event].remove(hook)

    def add_rate_limit(self, limiter: RateLimit):
        """ Add a RateLimit to the connection """
        if limiter not in self.rate_limiters:
            self.subscribe("capacity", limiter.on_capacity)
            self.rate_limiters.append(limiter)

    def remove_rate_limit(self, limiter: RateLimit):
        """ Remove a RateLimit from the connection """
        if limiter in self.rate_limiters:
            self.unsubscribe("capacity", limiter.on_capacity)
            self.rate_limiters.remove(limiter)

    @contextmanager
    def limit(self, limiter: RateLimit):
        """ Context manager that applies a RateLimit to the connection """
        self.add_rate_limit(limiter)
        try:
            yield
        finally:
            self.remove_rate_limit(limiter)

    def clear_hooks(self):
        """ Remove all hooks from all events """
        self._hooks = {
            "precall": [],
            "postcall": [],
            "capacity": [],
        }

    def _default_capacity(
        self, value: Optional[ReturnCapacityType]
    ) -> ReturnCapacityType:
        """ Get the value for ReturnConsumedCapacity from provided value """
        if value is not None:
            return value
        if self.default_return_capacity or self.rate_limiters:
            return INDEXES
        return NONE

    def _count(self, method: str, limit: Limit, keywords: Dict[str, Any]) -> Count:
        """ Do a scan or query and aggregate the results into a Count """
        # The limit will be mutated, so copy it and leave the original intact
        limit = limit.copy()
        has_more = True
        count = Count(0, 0)
        while has_more:
            limit.set_request_args(keywords)
            response = self.call(method, **keywords)
            limit.post_fetch(response)
            count += Count.from_response(response)
            last_evaluated_key = response.get("LastEvaluatedKey")
            has_more = last_evaluated_key is not None and not limit.complete
            if has_more:
                keywords["ExclusiveStartKey"] = last_evaluated_key
        return count

    def list_tables(self, limit: Optional[int] = None) -> TableResultSet:
        """
        List all tables.

        Parameters
        ----------
        limit : int, optional
            Maximum number of tables to return

        Returns
        -------
        tables : Iterator
            Iterator that returns table names as strings

        """
        return TableResultSet(self, limit)

    def describe_table(self, tablename: str) -> Optional[Table]:
        """
        Get the details about a table

        Parameters
        ----------
        tablename : str
            Name of the table

        Returns
        -------
        table : :class:`~dynamo3.fields.Table`

        """
        try:
            response = self.call("describe_table", TableName=tablename)["Table"]
            return Table.from_response(response)
        except DynamoDBError as e:
            if e.kwargs["Code"] == "ResourceNotFoundException":
                return None
            else:  # pragma: no cover
                raise

    def create_table(
        self,
        tablename: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        indexes: Optional[List[LocalIndex]] = None,
        global_indexes: Optional[List[GlobalIndex]] = None,
        throughput: Optional[Throughput] = None,
        wait: bool = False,
    ):
        """
        Create a table

        Parameters
        ----------
        tablename : str
            Name of the table
        hash_key : :class:`~dynamo3.fields.DynamoKey`
            The key to use as the Hash key
        range_key : :class:`~dynamo3.fields.DynamoKey`, optional
            The key to use as the Range key
        indexes : list, optional
            List of :class:`~dynamo3.fields.LocalIndex`
        global_indexes : list, optional
            List of :class:`~dynamo3.fields.GlobalIndex`
        throughput : :class:`~dynamo3.fields.Throughput`, optional
            The throughput of the table

        """
        if throughput is None:
            throughput = Throughput()
        all_attrs = set([hash_key])
        if range_key is not None:
            all_attrs.add(range_key)
        key_schema = [hash_key.hash_schema()]
        if range_key is not None:
            key_schema.append(range_key.range_schema())

        kwargs = {
            "TableName": tablename,
            "KeySchema": key_schema,
            "ProvisionedThroughput": throughput.schema(),
        }
        if indexes:
            kwargs["LocalSecondaryIndexes"] = [idx.schema(hash_key) for idx in indexes]
            for idx in indexes:
                all_attrs.add(idx.range_key)

        if global_indexes:
            kwargs["GlobalSecondaryIndexes"] = [
                gidx.schema() for gidx in global_indexes
            ]
            for gidx in global_indexes:
                all_attrs.add(gidx.hash_key)
                if gidx.range_key is not None:
                    all_attrs.add(gidx.range_key)

        kwargs["AttributeDefinitions"] = [attr.definition() for attr in all_attrs]
        result = self.call("create_table", **kwargs)
        if wait:
            self.client.get_waiter("table_exists").wait(TableName=tablename)
        return result

    def delete_table(self, tablename: str, wait: bool = False) -> bool:
        """
        Delete a table

        Parameters
        ----------
        tablename : str
            Name of the table to delete

        Returns
        -------
        response : bool
            True if the table was deleted, False if no table exists

        """
        try:
            self.call("delete_table", TableName=tablename)
            if wait:
                self.client.get_waiter("table_not_exists").wait(TableName=tablename)
            return True
        except DynamoDBError as e:
            if e.kwargs["Code"] == "ResourceNotFoundException":
                return False
            else:  # pragma: no cover
                raise

    def put_item2(
        self,
        tablename: str,
        item: DynamoObject,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        condition: Optional[str] = None,
        returns: Literal[Literal["NONE"], Literal["ALL_OLD"]] = NONE,
        return_capacity: ReturnCapacityType = None,
        return_item_collection_metrics=NONE,
        **kwargs: ExpressionValueType
    ):
        """
        Put a new item into a table

        For many parameters you will want to reference the DynamoDB API:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html

        Parameters
        ----------
        tablename : str
            Name of the table to update
        item : dict
            Item data
        expr_values : dict, optional
            See docs for ExpressionAttributeValues. See also: kwargs
        alias : dict, optional
            See docs for ExpressionAttributeNames
        condition : str, optional
            See docs for ConditionExpression
        returns : {NONE, ALL_OLD}, optional
            Return nothing or the old values from the item that was
            overwritten, if any (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        return_item_collection_metrics : (NONE, SIZE), optional
            SIZE will return statistics about item collections that were
            modified.
        **kwargs : dict, optional
            If expr_values is not provided, the kwargs dict will be used as the
            ExpressionAttributeValues (a ':' will be automatically prepended to
            all keys).

        """
        keywords = {
            "TableName": tablename,
            "Item": self.dynamizer.encode_keys(item),
            "ReturnValues": returns,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
            "ReturnItemCollectionMetrics": return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords["ExpressionAttributeValues"] = values
        if alias:
            keywords["ExpressionAttributeNames"] = alias
        if condition:
            keywords["ConditionExpression"] = condition
        result = self.call("put_item", **keywords)
        if result:
            return Result(self.dynamizer, result, "Attributes")

    def get_item(
        self, tablename, key, attributes=None, consistent=False, return_capacity=None
    ):
        """
        Fetch a single item from a table

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.get_item2`.

        Parameters
        ----------
        tablename : str
            Name of the table to fetch from
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        attributes : list, optional
            If present, only fetch these attributes from the item
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        kwargs = {
            "TableName": tablename,
            "Key": self.dynamizer.encode_keys(key),
            "ConsistentRead": consistent,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
        }
        if attributes is not None:
            kwargs["AttributesToGet"] = attributes
        data = self.call("get_item", **kwargs)
        return Result(self.dynamizer, data, "Item")

    def get_item2(
        self,
        tablename: str,
        key: DynamoObject,
        attributes: Optional[Union[str, List[str]]] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        consistent: bool = False,
        return_capacity: Optional[ReturnCapacityType] = None,
    ) -> Result:
        """
        Fetch a single item from a table

        Parameters
        ----------
        tablename : str
            Name of the table to fetch from
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        attributes : str or list, optional
            See docs for ProjectionExpression. If list, it will be joined by
            commas.
        alias : dict, optional
            See docs for ExpressionAttributeNames
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        kwargs = {
            "TableName": tablename,
            "Key": self.dynamizer.encode_keys(key),
            "ConsistentRead": consistent,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
        }
        if attributes is not None:
            if not isinstance(attributes, str):
                attributes = ", ".join(attributes)
            kwargs["ProjectionExpression"] = attributes
        if alias:
            kwargs["ExpressionAttributeNames"] = alias
        data = self.call("get_item", **kwargs)
        return Result(self.dynamizer, data, "Item")

    def delete_item2(
        self,
        tablename: str,
        key: DynamoObject,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        condition: Optional[str] = None,
        returns: Literal[Literal["NONE"], Literal["ALL_OLD"]] = NONE,
        return_capacity: Optional[ReturnCapacityType] = None,
        return_item_collection_metrics: ReturnItemCollectionMetricsType = NONE,
        **kwargs
    ) -> Optional[Result]:
        """
        Delete an item from a table

        For many parameters you will want to reference the DynamoDB API:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html

        Parameters
        ----------
        tablename : str
            Name of the table to update
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        expr_values : dict, optional
            See docs for ExpressionAttributeValues. See also: kwargs
        alias : dict, optional
            See docs for ExpressionAttributeNames
        condition : str, optional
            See docs for ConditionExpression
        returns : {NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW}, optional
            Return either the old or new values, either all attributes or just
            the ones that changed. (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        return_item_collection_metrics : (NONE, SIZE), optional
            SIZE will return statistics about item collections that were
            modified.
        **kwargs : dict, optional
            If expr_values is not provided, the kwargs dict will be used as the
            ExpressionAttributeValues (a ':' will be automatically prepended to
            all keys).

        """
        keywords = {
            "TableName": tablename,
            "Key": self.dynamizer.encode_keys(key),
            "ReturnValues": returns,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
            "ReturnItemCollectionMetrics": return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords["ExpressionAttributeValues"] = values
        if alias:
            keywords["ExpressionAttributeNames"] = alias
        if condition:
            keywords["ConditionExpression"] = condition
        result = self.call("delete_item", **keywords)
        if result:
            return Result(self.dynamizer, result, "Attributes")
        return None

    def batch_write(
        self,
        tablename: str,
        return_capacity: Optional[ReturnCapacityType] = None,
        return_item_collection_metrics: ReturnItemCollectionMetricsType = NONE,
    ) -> BatchWriter:
        """
        Perform a batch write on a table

        Parameters
        ----------
        tablename : str
            Name of the table to write to
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        return_item_collection_metrics : (NONE, SIZE), optional
            SIZE will return statistics about item collections that were
            modified.

        Examples
        --------
        .. code-block:: python

            with connection.batch_write('mytable') as batch:
                batch.put({'id': 'id1', 'foo': 'bar'})
                batch.delete({'id': 'oldid'})

        """
        return_capacity = self._default_capacity(return_capacity)
        return BatchWriter(
            self,
            tablename,
            return_capacity=return_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
        )

    def batch_get(
        self,
        tablename: str,
        keys: Iterable[DynamoObject],
        attributes: Optional[Union[str, List[str]]] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        consistent: bool = False,
        return_capacity: Optional[ReturnCapacityType] = None,
    ) -> GetResultSet:
        """
        Perform a batch get of many items in a table

        Parameters
        ----------
        tablename : str
            Name of the table to fetch from
        keys : list or iterable
            List or iterable of primary key dicts that specify the hash key and
            the optional range key of each item to fetch
        attributes : str or list, optional
            See docs for ProjectionExpression. If list, it will be joined by
            commas.
        alias : dict, optional
            See docs for ExpressionAttributeNames
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        keys = [self.dynamizer.encode_keys(k) for k in keys]
        return_capacity = self._default_capacity(return_capacity)
        ret = GetResultSet(
            self,
            tablename,
            keys,
            consistent=consistent,
            attributes=attributes,
            alias=alias,
            return_capacity=return_capacity,
        )
        return ret

    def update_item2(
        self,
        tablename,
        key,
        expression,
        expr_values=None,
        alias=None,
        condition=None,
        returns=NONE,
        return_capacity=None,
        return_item_collection_metrics=NONE,
        **kwargs
    ) -> Optional[Result]:
        """
        Update a single item in a table

        For many parameters you will want to reference the DynamoDB API:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html

        Parameters
        ----------
        tablename : str
            Name of the table to update
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        expression : str
            See docs for UpdateExpression
        expr_values : dict, optional
            See docs for ExpressionAttributeValues. See also: kwargs
        alias : dict, optional
            See docs for ExpressionAttributeNames
        condition : str, optional
            See docs for ConditionExpression
        returns : {NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW}, optional
            Return either the old or new values, either all attributes or just
            the ones that changed. (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        return_item_collection_metrics : (NONE, SIZE), optional
            SIZE will return statistics about item collections that were
            modified.
        **kwargs : dict, optional
            If expr_values is not provided, the kwargs dict will be used as the
            ExpressionAttributeValues (a ':' will be automatically prepended to
            all keys).

        """
        keywords = {
            "TableName": tablename,
            "Key": self.dynamizer.encode_keys(key),
            "UpdateExpression": expression,
            "ReturnValues": returns,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
            "ReturnItemCollectionMetrics": return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords["ExpressionAttributeValues"] = values
        if alias:
            keywords["ExpressionAttributeNames"] = alias
        if condition:
            keywords["ConditionExpression"] = condition
        result = self.call("update_item", **keywords)
        if result:
            return Result(self.dynamizer, result, "Attributes")
        return None

    @overload
    def scan2(
        self,
        tablename: str,
        expr_values: Optional[ExpressionValuesType],
        alias: Optional[ExpressionAttributeNamesType],
        attributes: Optional[Union[str, List[str]]],
        consistent: bool,
        select: Literal["COUNT"],
        index: Optional[str],
        limit: Optional[Union[Limit, int]],
        return_capacity: Optional[ReturnCapacityType],
        filter: Optional[str],
        segment: Optional[int],
        total_segments: Optional[int],
        exclusive_start_key: Optional[DynamoObject],
        **kwargs: ExpressionValueType
    ) -> Count:
        ...

    @overload
    def scan2(
        self,
        tablename: str,
        expr_values: Optional[ExpressionValuesType],
        alias: Optional[ExpressionAttributeNamesType],
        attributes: Optional[Union[str, List[str]]],
        consistent: bool,
        select: Optional[NonCountSelectType],
        index: Optional[str],
        limit: Optional[Union[Limit, int]],
        return_capacity: Optional[ReturnCapacityType],
        filter: Optional[str],
        segment: Optional[int],
        total_segments: Optional[int],
        exclusive_start_key: Optional[DynamoObject],
        **kwargs: ExpressionValueType
    ) -> ResultSet:
        ...

    def scan2(
        self,
        tablename: str,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        attributes: Optional[Union[str, List[str]]] = None,
        consistent: bool = False,
        select: Optional[SelectType] = None,
        index: Optional[str] = None,
        limit: Optional[Union[Limit, int]] = None,
        return_capacity: Optional[ReturnCapacityType] = None,
        filter: Optional[str] = None,
        segment: Optional[int] = None,
        total_segments: Optional[int] = None,
        exclusive_start_key: Optional[DynamoObject] = None,
        **kwargs: ExpressionValueType
    ) -> Union[Count, ResultSet]:
        """
        Perform a full-table scan

        For many parameters you will want to reference the DynamoDB API:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html

        Parameters
        ----------
        tablename : str
            Name of the table to scan
        expr_values : dict, optional
            See docs for ExpressionAttributeValues. See also: kwargs
        alias : dict, optional
            See docs for ExpressionAttributeNames
        attributes : str or list, optional
            See docs for ProjectionExpression. If list, it will be joined by
            commas.
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        select : str, optional
            See docs for Select
        index : str, optional
            The name of the index to query
        limit : int or Limit, optional
            Maximum number of items to return
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        filter : str, optional
            See docs for FilterExpression
        segment : int, optional
            When doing a parallel scan, the unique thread identifier for this
            scan. If present, total_segments must also be present.
        total_segments : int, optional
            When doing a parallel scan, the total number of threads performing
            the scan.
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query
        **kwargs : dict, optional
            If expr_values is not provided, the kwargs dict will be used as the
            ExpressionAttributeValues (a ':' will be automatically prepended to
            all keys).

        Examples
        --------

        .. code-block:: python

            connection.scan2('mytable', filter='contains(tags, :search)', search='text)
            connection.scan2('mytable', filter='id = :id', expr_values={':id': 'dsa'})

        """
        keywords = {
            "TableName": tablename,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
            "ConsistentRead": consistent,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords["ExpressionAttributeValues"] = values
        if attributes is not None:
            if not isinstance(attributes, str):
                attributes = ", ".join(attributes)
            keywords["ProjectionExpression"] = attributes
        if index is not None:
            keywords["IndexName"] = index
        if alias:
            keywords["ExpressionAttributeNames"] = alias
        if select:
            keywords["Select"] = select
        if filter:
            keywords["FilterExpression"] = filter
        if segment is not None:
            keywords["Segment"] = segment
        if total_segments is not None:
            keywords["TotalSegments"] = total_segments
        if exclusive_start_key is not None:
            keywords["ExclusiveStartKey"] = self.dynamizer.maybe_encode_keys(
                exclusive_start_key
            )
        if not isinstance(limit, Limit):
            limit = Limit(limit)

        if select == COUNT:
            return self._count("scan", limit, keywords)
        else:
            return ResultSet(self, limit, "scan", **keywords)

    @overload
    def query2(
        self,
        tablename: str,
        key_condition_expr: str,
        expr_values: Optional[DynamoObject],
        alias: Optional[ExpressionAttributeNamesType],
        attributes: Optional[Union[str, List[str]]],
        consistent: bool,
        select: Literal["COUNT"],
        index: Optional[str],
        limit: Optional[Union[int, Limit]],
        desc: bool,
        return_capacity: Optional[ReturnCapacityType],
        filter: Optional[str],
        exclusive_start_key: Optional[DynamoObject],
        **kwargs: ExpressionValueType
    ) -> Count:
        ...

    @overload
    def query2(
        self,
        tablename: str,
        key_condition_expr: str,
        expr_values: Optional[DynamoObject],
        alias: Optional[ExpressionAttributeNamesType],
        attributes: Optional[Union[str, List[str]]],
        consistent: bool,
        select: Optional[NonCountSelectType],
        index: Optional[str],
        limit: Optional[Union[int, Limit]],
        desc: bool,
        return_capacity: Optional[ReturnCapacityType],
        filter: Optional[str],
        exclusive_start_key: Optional[DynamoObject],
        **kwargs: ExpressionValueType
    ) -> ResultSet:
        ...

    def query2(
        self,
        tablename: str,
        key_condition_expr: str,
        expr_values: Optional[DynamoObject] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        attributes: Optional[Union[str, List[str]]] = None,
        consistent: bool = False,
        select: Optional[SelectType] = None,
        index: Optional[str] = None,
        limit: Optional[Union[int, Limit]] = None,
        desc: bool = False,
        return_capacity: Optional[ReturnCapacityType] = None,
        filter: Optional[str] = None,
        exclusive_start_key: Optional[DynamoObject] = None,
        **kwargs: ExpressionValueType
    ) -> Union[Count, ResultSet]:
        """
        Perform an index query on a table

        For many parameters you will want to reference the DynamoDB API:
        http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html

        Parameters
        ----------
        tablename : str
            Name of the table to query
        key_condition_expr : str
            See docs for KeyConditionExpression
        expr_values : dict, optional
            See docs for ExpressionAttributeValues. See also: kwargs
        alias : dict, optional
            See docs for ExpressionAttributeNames
        attributes : str or list, optional
            See docs for ProjectionExpression. If list, it will be joined by
            commas.
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        select : str, optional
            See docs for Select
        index : str, optional
            The name of the index to query
        limit : int, optional
            Maximum number of items to return
        desc : bool, optional
            If True, return items in descending order (default False)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        filter : str, optional
            See docs for FilterExpression
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query
        **kwargs : dict, optional
            If expr_values is not provided, the kwargs dict will be used as the
            ExpressionAttributeValues (a ':' will be automatically prepended to
            all keys).

        Examples
        --------

        .. code-block:: python

            connection.query2('mytable', 'foo = :foo', foo=5)
            connection.query2('mytable', 'foo = :foo', expr_values={':foo': 5})

        """
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        keywords = {
            "TableName": tablename,
            "ReturnConsumedCapacity": self._default_capacity(return_capacity),
            "ConsistentRead": consistent,
            "KeyConditionExpression": key_condition_expr,
            "ExpressionAttributeValues": values,
            "ScanIndexForward": not desc,
        }
        if attributes is not None:
            if not isinstance(attributes, str):
                attributes = ", ".join(attributes)
            keywords["ProjectionExpression"] = attributes
        if index is not None:
            keywords["IndexName"] = index
        if alias:
            keywords["ExpressionAttributeNames"] = alias
        if select:
            keywords["Select"] = select
        if filter:
            keywords["FilterExpression"] = filter
        if exclusive_start_key is not None:
            keywords["ExclusiveStartKey"] = self.dynamizer.maybe_encode_keys(
                exclusive_start_key
            )
        if not isinstance(limit, Limit):
            limit = Limit(limit)

        if select == COUNT:
            return self._count("query", limit, keywords)
        else:
            return ResultSet(self, limit, "query", **keywords)

    def update_table(
        self,
        tablename: str,
        throughput: Optional[Throughput] = None,
        index_updates: Optional[List[IndexUpdate]] = None,
    ):
        """
        Update the throughput of a table and/or global indexes

        Parameters
        ----------
        tablename : str
            Name of the table to update
        throughput : :class:`~dynamo3.fields.Throughput`, optional
            The new throughput of the table
        index_updates : list of :class:`~dynamo3.fields.IndexUpdate`, optional
            List of IndexUpdates to perform

        """
        kwargs: Dict[str, Any] = {"TableName": tablename}
        all_attrs = set()
        if throughput is not None:
            kwargs["ProvisionedThroughput"] = throughput.schema()
        if index_updates is not None:
            updates = []
            for update in index_updates:
                all_attrs.update(update.get_attrs())
                updates.append(update.serialize())
            kwargs["GlobalSecondaryIndexUpdates"] = updates
        if all_attrs:
            attr_definitions = [attr.definition() for attr in all_attrs]
            kwargs["AttributeDefinitions"] = attr_definitions
        return self.call("update_table", **kwargs)
