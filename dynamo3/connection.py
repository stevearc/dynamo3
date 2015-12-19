""" Connection class for DynamoDB """
from contextlib import contextmanager
import time
import warnings

import botocore.session
import six
from botocore.exceptions import ClientError

from .batch import BatchWriter, encode_query_kwargs
from .constants import NONE, COUNT, INDEXES, READ_COMMANDS
from .exception import translate_exception, DynamoDBError, ThroughputException
from .fields import Throughput, Table
from .result import (ResultSet, GetResultSet, Result, Count, ConsumedCapacity,
                     TableResultSet, Limit)
from .types import Dynamizer, is_null


def build_expected(dynamizer, expected):
    """ Build the Expected parameters from a dict """
    ret = {}
    for k, v in six.iteritems(expected):
        if is_null(v):
            ret[k] = {
                'Exists': False,
            }
        else:
            ret[k] = {
                'Exists': True,
                'Value': dynamizer.encode(v),
            }
    return ret


def build_expression_values(dynamizer, expr_values, kwargs):
    """ Build ExpresionAttributeValues from a value or kwargs """
    if expr_values:
        values = expr_values
        return dynamizer.encode_keys(values)
    elif kwargs:
        values = dict(((':' + k, v) for k, v in six.iteritems(kwargs)))
        return dynamizer.encode_keys(values)


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

    def __init__(self, client=None, dynamizer=Dynamizer()):
        self.client = client
        self.dynamizer = dynamizer
        self.request_retries = 10
        self.default_return_capacity = False
        self._hooks = None
        self.clear_hooks()
        self.rate_limiters = []

    @property
    def host(self):
        """ The address of the endpoint """
        return self.client.meta.endpoint_url

    @property
    def region(self):
        """ The name of the current connected region """
        return self.client.meta.region_name

    @classmethod
    def connect_to_region(cls, region, session=None, access_key=None,
                          secret_key=None, **kwargs):
        """
        Connect to an AWS region.

        This method has been deprecated in favor of :meth:`~.connect`

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
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        warnings.warn("connect_to_region is deprecated and will be removed. "
                      "Use connect instead.")
        if session is None:
            session = botocore.session.get_session()
            if access_key is not None:
                session.set_credentials(access_key, secret_key)
        client = session.create_client('dynamodb', region)
        return cls(client, **kwargs)

    @classmethod
    def connect_to_host(cls, host='localhost', port=8000, is_secure=False,
                        session=None, access_key=None, secret_key=None,
                        **kwargs):
        """
        Connect to a specific host.

        This method has been deprecated in favor of :meth:`~.connect`

        Parameters
        ----------
        host : str, optional
            Address of the host (default 'localhost')
        port : int, optional
            Connect to the host on this port (default 8000)
        is_secure : bool, optional
            Enforce https connection (default False)
        session : :class:`~botocore.session.Session`, optional
            The Session object to use for the connection
        access_key : str, optional
            If session is None, set this access key when creating the session
        secret_key : str, optional
            If session is None, set this secret key when creating the session
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        warnings.warn("connect_to_host is deprecated and will be removed. "
                      "Use connect instead.")
        if session is None:
            session = botocore.session.get_session()
            if access_key is not None:
                session.set_credentials(access_key, secret_key)
        url = "http://%s:%d" % (host, port)
        client = session.create_client('dynamodb', 'local', endpoint_url=url,
                                       use_ssl=is_secure)
        return cls(client, **kwargs)

    @classmethod
    def connect(cls, region, session=None, access_key=None, secret_key=None,
                host=None, port=80, is_secure=True, **kwargs):
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
            protocol = 'https' if is_secure else 'http'
            url = "%s://%s:%d" % (protocol, host, port)
        client = session.create_client('dynamodb', region, endpoint_url=url,
                                       use_ssl=is_secure)
        return cls(client, **kwargs)

    def call(self, command, **kwargs):
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
        for hook in self._hooks['precall']:
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
        for hook in self._hooks['postcall']:
            hook(self, command, kwargs, data)
        if 'ConsumedCapacity' in data:
            is_read = command in READ_COMMANDS
            consumed = data['ConsumedCapacity']
            if isinstance(consumed, list):
                data['consumed_capacity'] = [
                    ConsumedCapacity.from_response(cap, is_read)
                    for cap in consumed
                ]
            else:
                capacity = ConsumedCapacity.from_response(consumed, is_read)
                data['consumed_capacity'] = capacity
        if 'consumed_capacity' in data:
            if isinstance(data['consumed_capacity'], list):
                all_caps = data['consumed_capacity']
            else:
                all_caps = [data['consumed_capacity']]
            for hook in self._hooks['capacity']:
                for cap in all_caps:
                    hook(self, command, kwargs, data, cap)
        return data

    def exponential_sleep(self, attempt):
        """ Sleep with exponential backoff """
        if attempt > 1:
            time.sleep(0.1 * 2 ** attempt)

    def subscribe(self, event, hook):
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

    def unsubscribe(self, event, hook):
        """ Unsubscribe a hook from an event """
        if hook in self._hooks[event]:
            self._hooks[event].remove(hook)

    def add_rate_limit(self, limiter):
        """ Add a RateLimit to the connection """
        if limiter not in self.rate_limiters:
            self.subscribe('capacity', limiter.on_capacity)
            self.rate_limiters.append(limiter)

    def remove_rate_limit(self, limiter):
        """ Remove a RateLimit from the connection """
        if limiter in self.rate_limiters:
            self.unsubscribe('capacity', limiter.on_capacity)
            self.rate_limiters.remove(limiter)

    @contextmanager
    def limit(self, limiter):
        """ Context manager that applies a RateLimit to the connection """
        self.add_rate_limit(limiter)
        try:
            yield
        finally:
            self.remove_rate_limit(limiter)

    def clear_hooks(self):
        """ Remove all hooks from all events """
        self._hooks = {
            'precall': [],
            'postcall': [],
            'capacity': [],
        }

    def _default_capacity(self, value):
        """ Get the value for ReturnConsumedCapacity from provided value """
        if value is not None:
            return value
        if self.default_return_capacity or self.rate_limiters:
            return INDEXES
        return NONE

    def _count(self, method, limit, keywords):
        """ Do a scan or query and aggregate the results into a Count """
        # The limit will be mutated, so copy it and leave the original intact
        limit = limit.copy()
        has_more = True
        count = None
        while has_more:
            limit.set_request_args(keywords)
            response = self.call(method, **keywords)
            limit.post_fetch(response)
            count += Count.from_response(response)
            last_evaluated_key = response.get('LastEvaluatedKey')
            has_more = last_evaluated_key is not None and not limit.complete
            if has_more:
                keywords['ExclusiveStartKey'] = last_evaluated_key
        return count

    def list_tables(self, limit=None):
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

    def describe_table(self, tablename):
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
            response = self.call(
                'describe_table', TableName=tablename)['Table']
            return Table.from_response(response)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return None
            else:  # pragma: no cover
                raise

    def create_table(self, tablename, hash_key, range_key=None,
                     indexes=None, global_indexes=None, throughput=None):
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
            'TableName': tablename,
            'KeySchema': key_schema,
            'ProvisionedThroughput': throughput.schema(),
        }
        if indexes:
            kwargs['LocalSecondaryIndexes'] = [
                idx.schema(hash_key) for idx in indexes
            ]
            for idx in indexes:
                all_attrs.add(idx.range_key)

        if global_indexes:
            kwargs['GlobalSecondaryIndexes'] = [
                idx.schema() for idx in global_indexes
            ]
            for idx in global_indexes:
                all_attrs.add(idx.hash_key)
                if idx.range_key is not None:
                    all_attrs.add(idx.range_key)

        kwargs['AttributeDefinitions'] = [attr.definition() for attr in
                                          all_attrs]
        return self.call('create_table', **kwargs)

    def delete_table(self, tablename):
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
            self.call('delete_table', TableName=tablename)
            return True
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return False
            else:  # pragma: no cover
                raise

    def put_item(self, tablename, item, expected=None, returns=NONE,
                 return_capacity=None, expect_or=False, **kwargs):
        """
        Store an item, overwriting existing data

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.put_item2`.

        Parameters
        ----------
        tablename : str
            Name of the table to write
        item : dict
            Item data
        expected : dict, optional
            DEPRECATED (use **kwargs instead).
            If present, will check the values in Dynamo before performing the
            write. If values do not match, will raise an exception. (Using None
            as a value checks that the field does not exist).
        returns : {NONE, ALL_OLD}, optional
            If ALL_OLD, will return any data that was overwritten (default
            NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        expect_or : bool, optional
            If True, the **kwargs conditionals will be OR'd together. If False,
            they will be AND'd. (default False).
        **kwargs : dict, optional
            Conditional filter on the PUT. Same format as the kwargs for
            :meth:`~.scan`.

        """
        keywords = {}
        if kwargs:
            keywords['Expected'] = encode_query_kwargs(self.dynamizer, kwargs)
            if len(keywords['Expected']) > 1:
                keywords['ConditionalOperator'] = 'OR' if expect_or else 'AND'
        elif expected is not None:
            keywords['Expected'] = build_expected(self.dynamizer, expected)
        keywords['ReturnConsumedCapacity'] = \
            self._default_capacity(return_capacity)
        item = self.dynamizer.encode_keys(item)
        ret = self.call('put_item', TableName=tablename, Item=item,
                        ReturnValues=returns, **keywords)
        if ret:
            return Result(self.dynamizer, ret, 'Attributes')

    def put_item2(self, tablename, item, expr_values=None, alias=None,
                  condition=None, returns=NONE, return_capacity=None,
                  return_item_collection_metrics=NONE, **kwargs):
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
            'TableName': tablename,
            'Item': self.dynamizer.encode_keys(item),
            'ReturnValues': returns,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ReturnItemCollectionMetrics': return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords['ExpressionAttributeValues'] = values
        if alias:
            keywords['ExpressionAttributeNames'] = alias
        if condition:
            keywords['ConditionExpression'] = condition
        result = self.call('put_item', **keywords)
        if result:
            return Result(self.dynamizer, result, 'Attributes')

    def get_item(self, tablename, key, attributes=None, consistent=False,
                 return_capacity=None):
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
            'TableName': tablename,
            'Key': self.dynamizer.encode_keys(key),
            'ConsistentRead': consistent,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
        }
        if attributes is not None:
            kwargs['AttributesToGet'] = attributes
        data = self.call('get_item', **kwargs)
        return Result(self.dynamizer, data, 'Item')

    def get_item2(self, tablename, key, attributes=None, alias=None,
                  consistent=False, return_capacity=None):
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
            'TableName': tablename,
            'Key': self.dynamizer.encode_keys(key),
            'ConsistentRead': consistent,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
        }
        if attributes is not None:
            if not isinstance(attributes, six.string_types):
                attributes = ', '.join(attributes)
            kwargs['ProjectionExpression'] = attributes
        if alias:
            kwargs['ExpressionAttributeNames'] = alias
        data = self.call('get_item', **kwargs)
        return Result(self.dynamizer, data, 'Item')

    def delete_item(self, tablename, key, expected=None, returns=NONE,
                    return_capacity=None, expect_or=False, **kwargs):
        """
        Delete an item

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.delete_item2`.

        Parameters
        ----------
        tablename : str
            Name of the table to delete from
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        expected : dict, optional
            DEPRECATED (use **kwargs instead).
            If present, will check the values in Dynamo before performing the
            write. If values do not match, will raise an exception. (Using None
            as a value checks that the field does not exist).
        returns : {NONE, ALL_OLD}, optional
            If ALL_OLD, return the data that was deleted (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        expect_or : bool, optional
            If True, the **kwargs conditionals will be OR'd together. If False,
            they will be AND'd. (default False).
        **kwargs : dict, optional
            Conditional filter on the DELETE. Same format as the kwargs for
            :meth:`~.scan`.

        """
        key = self.dynamizer.encode_keys(key)
        keywords = {
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
        }
        if kwargs:
            keywords['Expected'] = encode_query_kwargs(self.dynamizer, kwargs)
            if len(keywords['Expected']) > 1:
                keywords['ConditionalOperator'] = 'OR' if expect_or else 'AND'
        elif expected is not None:
            keywords['Expected'] = build_expected(self.dynamizer, expected)
        ret = self.call('delete_item', TableName=tablename, Key=key,
                        ReturnValues=returns, **keywords)
        if ret:
            return Result(self.dynamizer, ret, 'Attributes')

    def delete_item2(self, tablename, key, expr_values=None, alias=None,
                     condition=None, returns=NONE, return_capacity=None,
                     return_item_collection_metrics=NONE, **kwargs):
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
            'TableName': tablename,
            'Key': self.dynamizer.encode_keys(key),
            'ReturnValues': returns,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ReturnItemCollectionMetrics': return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords['ExpressionAttributeValues'] = values
        if alias:
            keywords['ExpressionAttributeNames'] = alias
        if condition:
            keywords['ConditionExpression'] = condition
        result = self.call('delete_item', **keywords)
        if result:
            return Result(self.dynamizer, result, 'Attributes')

    def batch_write(self, tablename, return_capacity=None,
                    return_item_collection_metrics=NONE):
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
        return BatchWriter(self, tablename, return_capacity=return_capacity,
                           return_item_collection_metrics=return_item_collection_metrics)

    def batch_get(self, tablename, keys, attributes=None, alias=None,
                  consistent=False, return_capacity=None):
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
        ret = GetResultSet(self, tablename, keys,
                           consistent=consistent, attributes=attributes,
                           alias=alias, return_capacity=return_capacity)
        return ret

    def update_item(self, tablename, key, updates, returns=NONE,
                    return_capacity=None, expect_or=False, **kwargs):
        """
        Update a single item in a table

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.update_item2`.

        Parameters
        ----------
        tablename : str
            Name of the table to update
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        updates : list
            List of :class:`~dynamo3.batch.ItemUpdate`
        returns : {NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW}, optional
            Return either the old or new values, either all attributes or just
            the ones that changed. (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        expect_or : bool, optional
            If True, the updates conditionals will be OR'd together. If False,
            they will be AND'd. (default False).
        **kwargs : dict, optional
            Conditional filter on the PUT. Same format as the kwargs for
            :meth:`~.scan`.

        Notes
        -----
        There are two ways to specify the expected values of fields. The
        simplest is via the list of updates. Each updated field may specify a
        constraint on the current value of that field. You may pass additional
        constraints in via the **kwargs the same way you would for put_item.
        This is necessary if you have constraints on fields that are not being
        updated.

        """
        key = self.dynamizer.encode_keys(key)
        attr_updates = {}
        expected = {}
        keywords = {
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
        }
        for update in updates:
            attr_updates.update(update.attrs(self.dynamizer))
            expected.update(update.expected(self.dynamizer))

        # Pull the 'expected' constraints from the kwargs
        for k, v in six.iteritems(encode_query_kwargs(self.dynamizer, kwargs)):
            if k in expected:
                raise ValueError("Cannot have more than one condition on a single field")
            expected[k] = v

        if expected:
            keywords['Expected'] = expected
            if len(expected) > 1:
                keywords['ConditionalOperator'] = 'OR' if expect_or else 'AND'

        result = self.call('update_item', TableName=tablename, Key=key,
                           AttributeUpdates=attr_updates,
                           ReturnValues=returns,
                           **keywords)
        if result:
            return Result(self.dynamizer, result, 'Attributes')

    def update_item2(self, tablename, key, expression, expr_values=None, alias=None,
                     condition=None, returns=NONE, return_capacity=None,
                     return_item_collection_metrics=NONE, **kwargs):
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
            'TableName': tablename,
            'Key': self.dynamizer.encode_keys(key),
            'UpdateExpression': expression,
            'ReturnValues': returns,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ReturnItemCollectionMetrics': return_item_collection_metrics,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords['ExpressionAttributeValues'] = values
        if alias:
            keywords['ExpressionAttributeNames'] = alias
        if condition:
            keywords['ConditionExpression'] = condition
        result = self.call('update_item', **keywords)
        if result:
            return Result(self.dynamizer, result, 'Attributes')

    def scan(self, tablename, attributes=None, count=False, limit=None,
             return_capacity=None, filter_or=False, exclusive_start_key=None,
             **kwargs):
        """
        Perform a full-table scan

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.scan2`.

        Parameters
        ----------
        tablename : str
            Name of the table to scan
        attributes : list
            If present, only fetch these attributes from the item
        count : bool, optional
            If True, return a count of matched items instead of the items
            themselves (default False)
        limit : int, optional
            Maximum number of items to return
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
        filter_or : bool, optional
            If True, multiple filter kwargs will be OR'd together. If False,
            they will be AND'd together. (default False)
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query
        **kwargs : dict, optional
            Filter arguments (examples below)

        Examples
        --------
        You may pass in constraints using the Django-style '__' syntax. For
        example:

        .. code-block:: python

            connection.scan('mytable', tags__contains='searchtag')
            connection.scan('mytable', name__eq='dsa')
            connection.scan('mytable', action__in=['wibble', 'wobble'])

        """
        keywords = {
            'TableName': tablename,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
        }
        if attributes is not None:
            keywords['AttributesToGet'] = attributes
        if exclusive_start_key is not None:
            keywords['ExclusiveStartKey'] = \
                self.dynamizer.maybe_encode_keys(exclusive_start_key)
        if kwargs:
            keywords['ScanFilter'] = encode_query_kwargs(
                self.dynamizer, kwargs)
            if len(kwargs) > 1:
                keywords['ConditionalOperator'] = 'OR' if filter_or else 'AND'
        if not isinstance(limit, Limit):
            limit = Limit(limit)
        if count:
            keywords['Select'] = COUNT
            return self._count('scan', limit, keywords)
        else:
            return ResultSet(self, limit, 'scan', **keywords)

    def scan2(self, tablename, expr_values=None, alias=None, attributes=None,
              consistent=False, select=None, index=None, limit=None,
              return_capacity=None, filter=False, segment=None,
              total_segments=None, exclusive_start_key=None, **kwargs):
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
        limit : int, optional
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
            'TableName': tablename,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ConsistentRead': consistent,
        }
        values = build_expression_values(self.dynamizer, expr_values, kwargs)
        if values:
            keywords['ExpressionAttributeValues'] = values
        if attributes is not None:
            if not isinstance(attributes, six.string_types):
                attributes = ', '.join(attributes)
            keywords['ProjectionExpression'] = attributes
        if index is not None:
            keywords['IndexName'] = index
        if alias:
            keywords['ExpressionAttributeNames'] = alias
        if select:
            keywords['Select'] = select
        if filter:
            keywords['FilterExpression'] = filter
        if segment is not None:
            keywords['Segment'] = segment
        if total_segments is not None:
            keywords['TotalSegments'] = total_segments
        if exclusive_start_key is not None:
            keywords['ExclusiveStartKey'] = \
                self.dynamizer.maybe_encode_keys(exclusive_start_key)
        if not isinstance(limit, Limit):
            limit = Limit(limit)

        if select == COUNT:
            return self._count('scan', limit, keywords)
        else:
            return ResultSet(self, limit, 'scan', **keywords)

    def query(self, tablename, attributes=None, consistent=False, count=False,
              index=None, limit=None, desc=False, return_capacity=None,
              filter=None, filter_or=False, exclusive_start_key=None, **kwargs):
        """
        Perform an index query on a table

        This uses the older version of the DynamoDB API.
        See also: :meth:`~.query2`.

        Parameters
        ----------
        tablename : str
            Name of the table to query
        attributes : list
            If present, only fetch these attributes from the item
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        count : bool, optional
            If True, return a count of matched items instead of the items
            themselves (default False)
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
        filter : dict, optional
            Query arguments. Same format as **kwargs, but these arguments
            filter the results on the server before they are returned. They
            will NOT use an index, as that is what the **kwargs are for.
        filter_or : bool, optional
            If True, multiple filter args will be OR'd together. If False, they
            will be AND'd together. (default False)
        exclusive_start_key : dict, optional
            The ExclusiveStartKey to resume a previous query
        **kwargs : dict, optional
            Query arguments (examples below)

        Examples
        --------
        You may pass in constraints using the Django-style '__' syntax. For
        example:

        .. code-block:: python

            connection.query('mytable', foo__eq=5)
            connection.query('mytable', foo__eq=5, bar__lt=22)
            connection.query('mytable', foo__eq=5, bar__between=(1, 10))

        """
        keywords = {
            'TableName': tablename,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ConsistentRead': consistent,
            'ScanIndexForward': not desc,
            'KeyConditions': encode_query_kwargs(self.dynamizer, kwargs),
        }
        if attributes is not None:
            keywords['AttributesToGet'] = attributes
        if index is not None:
            keywords['IndexName'] = index
        if filter is not None:
            if len(filter) > 1:
                keywords['ConditionalOperator'] = 'OR' if filter_or else 'AND'
            keywords['QueryFilter'] = encode_query_kwargs(self.dynamizer,
                                                          filter)
        if exclusive_start_key is not None:
            keywords['ExclusiveStartKey'] = \
                self.dynamizer.maybe_encode_keys(exclusive_start_key)
        if not isinstance(limit, Limit):
            limit = Limit(limit)
        if count:
            keywords['Select'] = COUNT
            return self._count('query', limit, keywords)
        else:
            return ResultSet(self, limit, 'query', **keywords)

    def query2(self, tablename, key_condition_expr, expr_values=None,
               alias=None, attributes=None, consistent=False, select=None,
               index=None, limit=None, desc=False, return_capacity=None,
               filter=None, exclusive_start_key=None, **kwargs):
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
            'TableName': tablename,
            'ReturnConsumedCapacity': self._default_capacity(return_capacity),
            'ConsistentRead': consistent,
            'KeyConditionExpression': key_condition_expr,
            'ExpressionAttributeValues': values,
            'ScanIndexForward': not desc,
        }
        if attributes is not None:
            if not isinstance(attributes, six.string_types):
                attributes = ', '.join(attributes)
            keywords['ProjectionExpression'] = attributes
        if index is not None:
            keywords['IndexName'] = index
        if alias:
            keywords['ExpressionAttributeNames'] = alias
        if select:
            keywords['Select'] = select
        if filter:
            keywords['FilterExpression'] = filter
        if exclusive_start_key is not None:
            keywords['ExclusiveStartKey'] = \
                self.dynamizer.maybe_encode_keys(exclusive_start_key)
        if not isinstance(limit, Limit):
            limit = Limit(limit)

        if select == COUNT:
            return self._count('query', limit, keywords)
        else:
            return ResultSet(self, limit, 'query', **keywords)

    def update_table(self, tablename, throughput=None, global_indexes=None,
                     index_updates=None):
        """
        Update the throughput of a table and/or global indexes

        Parameters
        ----------
        tablename : str
            Name of the table to update
        throughput : :class:`~dynamo3.fields.Throughput`, optional
            The new throughput of the table
        global_indexes : dict, optional
            DEPRECATED. Use index_updates now.
            Map of index name to :class:`~dynamo3.fields.Throughput`
        index_updates : list of :class:`~dynamo3.fields.IndexUpdate`, optional
            List of IndexUpdates to perform

        """
        kwargs = {
            'TableName': tablename
        }
        all_attrs = set()
        if throughput is not None:
            kwargs['ProvisionedThroughput'] = throughput.schema()
        if index_updates is not None:
            updates = []
            for update in index_updates:
                all_attrs.update(update.get_attrs())
                updates.append(update.serialize())
            kwargs['GlobalSecondaryIndexUpdates'] = updates
        elif global_indexes is not None:
            kwargs['GlobalSecondaryIndexUpdates'] = [
                {
                    'Update': {
                        'IndexName': key,
                        'ProvisionedThroughput': value.schema(),
                    }
                }
                for key, value in six.iteritems(global_indexes)
            ]
        if all_attrs:
            attr_definitions = [attr.definition() for attr in all_attrs]
            kwargs['AttributeDefinitions'] = attr_definitions
        return self.call('update_table', **kwargs)
