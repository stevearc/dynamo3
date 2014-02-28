""" An API that wraps DynamoDB calls """
import botocore.session
import logging
import six
from pprint import pformat
from six.moves import xrange as _xrange  # pylint: disable=F0401

from .fields import Throughput, Table, DynamoKey, LocalIndex, GlobalIndex
from .types import (Dynamizer, Binary, STRING, NUMBER, BINARY, STRING_SET,
                    NUMBER_SET, BINARY_SET)


try:
    from ._version import __version__
except ImportError:  # pragma: no cover
    __version__ = 'unknown'

LOG = logging.getLogger(__name__)

CONDITIONS = {
    'eq': 'EQ',
    'le': 'LE',
    'lte': 'LE',
    'lt': 'LT',
    'ge': 'GE',
    'gte': 'GE',
    'gt': 'GT',
    'beginswith': 'BEGINS_WITH',
    'begins_with': 'BEGINS_WITH',
    'between': 'BETWEEN',
    'in': 'IN',
    'contains': 'CONTAINS',
    'ncontains': 'NOT_CONTAINS',
}

NONE = 'NONE'

# ReturnValues
ALL_OLD = 'ALL_OLD'
ALL_NEW = 'ALL_NEW'
UPDATED_OLD = 'UPDATED_OLD'
UPDATED_NEW = 'UPDATED_NEW'

# ReturnConsumedCapacity
INDEXES = 'INDEXES'
TOTAL = 'TOTAL'


def encode_query_kwargs(dynamizer, kwargs):
    """ Encode query constraints in Dynamo format """
    ret = {}
    for k, v in six.iteritems(kwargs):
        if '__' not in k:
            raise TypeError("Invalid query argument '%s'" % k)
        name, condition_key = k.split('__')
        # null is a special case
        if condition_key == 'null':
            ret[name] = {
                'ComparisonOperator': 'NULL' if v else 'NOT_NULL'
            }
            continue
        elif condition_key != 'in' and not isinstance(v, (list, tuple)):
            v = (v,)
        ret[name] = {
            'AttributeValueList': [dynamizer.encode(value) for value in v],
            'ComparisonOperator': CONDITIONS[condition_key],
        }
    return ret


class DynamoDBError(botocore.exceptions.BotoCoreError):

    """ Base error that we get back from Dynamo """
    fmt = '{Code}: {Message}\nArgs: {args}'

    def __init__(self, status_code, **kwargs):
        self.status_code = status_code
        super(DynamoDBError, self).__init__(**kwargs)


class ConditionalCheckFailedException(DynamoDBError):

    """ Raised when an item field value fails the expected value check """

    fmt = '{Code}: {Message}'

    def __init__(self, status_code, **kwargs):
        super(ConditionalCheckFailedException, self).__init__(status_code,
                                                              **kwargs)

CheckFailed = ConditionalCheckFailedException

EXC = {
    'ConditionalCheckFailedException': ConditionalCheckFailedException,
}


class BatchWriter(object):

    """ Context manager for writing a large number of items to a table """

    def __init__(self, connection, tablename):
        self.connection = connection
        self.tablename = tablename
        self._to_put = []
        self._to_delete = []
        self._unprocessed = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self._to_put or self._to_delete:
            # Flush anything that's left.
            self.flush()

        if self._unprocessed:
            # Finally, handle anything that wasn't processed.
            self.resend_unprocessed()

    def put(self, data):
        """
        Write an item (will overwrite existing data)

        Parameters
        ----------
        data : dict
            Item data

        """
        self._to_put.append(data)

        if self.should_flush():
            self.flush()

    def delete(self, **kwargs):
        """
        Delete an item

        Parameters
        ----------
        **kwargs : dict
            The primary key of the item to delete

        """
        self._to_delete.append(kwargs)

        if self.should_flush():
            self.flush()

    def should_flush(self):
        """ True if a flush is needed """
        return len(self._to_put) + len(self._to_delete) == 25

    def flush(self):
        """ Flush pending items to Dynamo """
        items = []

        for data in self._to_put:
            items.append(ItemPut(data))

        for data in self._to_delete:
            items.append(ItemDelete(data))

        resp = self._batch_write_item(items)
        self._handle_unprocessed(resp)

        self._to_put = []
        self._to_delete = []
        return True

    def _handle_unprocessed(self, resp):
        """ Requeue unprocessed items """
        if len(resp.get('UnprocessedItems', [])):
            unprocessed = resp['UnprocessedItems'].get(self.tablename, [])

            # Some items have not been processed. Stow them for now &
            # re-attempt processing on ``__exit__``.
            LOG.info("%d items were unprocessed. Storing for later.",
                     len(unprocessed))
            self._unprocessed.extend(unprocessed)

    def resend_unprocessed(self):
        """ Resend all unprocessed items """
        LOG.info("Re-sending %s unprocessed items.", len(self._unprocessed))

        while len(self._unprocessed):
            # Again, do 25 at a time.
            to_resend = self._unprocessed[:25]
            # Remove them from the list.
            self._unprocessed = self._unprocessed[25:]
            LOG.info("Sending %s items", len(to_resend))
            resp = self._batch_write_item(to_resend)
            self._handle_unprocessed(resp)
            LOG.info("%s unprocessed items left", len(self._unprocessed))

    def _batch_write_item(self, items):
        """ Make a BatchWriteItem call to Dynamo """
        data = {
            self.tablename: [
                item.encode(self.connection.dynamizer) for item in items
            ]
        }
        return self.connection.call('BatchWriteItem', request_items=data)


def is_null(value):
    """ Check if a value is equivalent to null in Dynamo """
    return value is None or value == set() or value == frozenset()


class ItemWrite(object):

    """ Base class for a single item write request """

    def __init__(self, data, action, key):
        self.data = data
        self.action = action
        self.key = key

    def encode(self, dynamizer):
        """ Encode the item write """
        # Strip null values out of data
        data = dict(((k, dynamizer.encode(v)) for k, v in
                     six.iteritems(self.data) if not is_null(v)))
        return {
            self.action: {
                self.key: data,
            }
        }


class ItemPut(ItemWrite):

    """ A write request that puts an item """

    def __init__(self, data):
        super(ItemPut, self).__init__(data, 'PutRequest', 'Item')


class ItemDelete(ItemWrite):

    """ A write request that deletes an item """

    def __init__(self, data):
        super(ItemDelete, self).__init__(data, 'DeleteRequest', 'Key')

NO_ARG = object()


class ItemUpdate(object):

    """
    An update operation for a single field on an item

    You should generally use the :meth:`~.put`, :meth:`~.add`, and
    :meth:`~.delete` methods instead of the constructor.

    Parameters
    ----------
    action : {ADD, DELETE, PUT}
        The action to take
    key : str
        The name of the field
    value : object, optional
        The new value for the field
    expected : object, optional
        The expected current value for the field (None expects value to not
        exist)

    """

    ADD = 'ADD'
    DELETE = 'DELETE'
    PUT = 'PUT'

    def __init__(self, action, key, value=None, expected=NO_ARG):
        if is_null(value):
            if action == self.ADD:
                raise ValueError("Update must set a value for non-delete "
                                 "operations!")
            elif action == self.PUT:
                # If we are trying to PUT a null value, change to a delete
                action = self.DELETE
        self.action = action
        self.key = key
        self.value = value
        self._expected = expected

    @classmethod
    def put(cls, *args, **kwargs):
        """ Shortcut for creating a PUT update """
        return cls(cls.PUT, *args, **kwargs)

    @classmethod
    def add(cls, *args, **kwargs):
        """ Shortcut for creating an ADD update """
        return cls(cls.ADD, *args, **kwargs)

    @classmethod
    def delete(cls, *args, **kwargs):
        """ Shortcut for creating a DELETE update """
        return cls(cls.DELETE, *args, **kwargs)

    def attrs(self, dynamizer):
        """ Get the attributes for the update """
        ret = {
            self.key: {
                'Action': self.action,
            }
        }
        if not is_null(self.value):
            ret[self.key]['Value'] = dynamizer.encode(self.value)
        return ret

    def expected(self, dynamizer):
        """ Get the expected values for the update """
        if self._expected is not NO_ARG:
            ret = {}
            if is_null(self._expected):
                ret['Exists'] = False
            else:
                ret['Value'] = dynamizer.encode(self._expected)
                ret['Exists'] = True
            return {self.key: ret}
        return {}


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


class ResultSet(six.Iterator):

    """ Iterator that pages results from Dynamo """

    def __init__(self, connection, response_key=None, *args, **kwargs):
        self.connection = connection
        self.response_key = response_key
        self.args = args
        self.kwargs = kwargs
        self.results = None
        self.last_evaluated_key = None
        self.cur_idx = 0

    @property
    def can_fetch_more(self):
        """ True if there are more results on the server """
        return (self.results is None or
                (self.last_evaluated_key is not None and
                 self.kwargs.get('limit', -1) != 0))

    def fetch(self):
        """ Fetch more results from Dynamo """
        self.cur_idx = 0
        data = self.connection.call(*self.args, **self.kwargs)
        self.results = data[self.response_key]
        self.last_evaluated_key = data.get('LastEvaluatedKey')
        if self.last_evaluated_key is not None:
            self.kwargs['exclusive_start_key'] = self.last_evaluated_key
        elif 'exclusive_start_key' in self.kwargs:
            del self.kwargs['exclusive_start_key']

    def __iter__(self):
        return self

    def __next__(self):
        if self.results is None or self.cur_idx >= len(self.results):
            if self.can_fetch_more:
                self.fetch()
        if self.cur_idx >= len(self.results):
            raise StopIteration
        result = self.results[self.cur_idx]
        self.cur_idx += 1
        if 'limit' in self.kwargs:
            self.kwargs['limit'] -= 1
        if isinstance(result, dict):
            return self.connection.dynamizer.decode_keys(result)
        return result


class GetResultSet(ResultSet):

    """ Iterator that pages the results of a BatchGetItem """

    def __init__(self, connection, tablename, keys, consistent, attributes,
                 return_capacity=NONE):
        super(GetResultSet, self).__init__(connection)
        self.tablename = tablename
        self.keys = keys
        self.consistent = consistent
        self.attributes = attributes
        self.return_capacity = return_capacity
        self.page_size = 100
        self.unprocessed_keys = []
        self.capacity = 0
        self.table_capacity = 0
        self.indexes = {}
        self.global_indexes = {}
        self.cur_page = 0

    def fetch_items(self, keys):
        """ Fetch a set of items from their keys """
        query = {'ConsistentRead': self.consistent}
        if self.attributes is not None:
            query['AttributesToGet'] = self.attributes
        query['Keys'] = keys
        kwargs = {
            'request_items': {
                self.tablename: query,
            },
            'return_consumed_capacity': self.return_capacity,
        }
        data = self.connection.call('BatchGetItem', **kwargs)
        for items in six.itervalues(data['UnprocessedKeys']):
            self.unprocessed_keys.extend(items['Keys'])
        cap = data.get('ConsumedCapacity', {})
        self.capacity += cap.get('CapacityUnits', 0)
        self.table_capacity += cap.get('Table', {}).get('CapacityUnits', 0)
        for k, v in six.iteritems(cap.get('LocalSecondaryIndexes', {})):
            self.indexes.setdefault(k, 0)
            self.indexes[k] += v['CapacityUnits']
        for k, v in six.iteritems(cap.get('GlobalSecondaryIndexes', {})):
            self.global_indexes.setdefault(k, 0)
            self.global_indexes[k] += v['CapacityUnits']
        self.results = data['Responses'][self.tablename]
        self.cur_idx = 0

    def __next__(self):
        if self.results is None or self.cur_idx >= len(self.results):
            self.results = None
            if self.cur_page * self.page_size < len(self.keys):
                start = self.cur_page * self.page_size
                end = min(start + self.page_size, len(self.keys))
                keys = []
                for i in _xrange(start, end):
                    key = self.keys[i]
                    keys.append(self.connection.dynamizer.encode_keys(key))
                self.fetch_items(keys)
                self.cur_page += 1
            elif self.unprocessed_keys:
                keys = self.unprocessed_keys[:self.page_size]
                self.unprocessed_keys = self.unprocessed_keys[self.page_size:]
                self.fetch_items(keys)
        if self.results is None or self.cur_idx >= len(self.results):
            raise StopIteration
        result = self.results[self.cur_idx]
        self.cur_idx += 1
        return self.connection.dynamizer.decode_keys(result)


class Result(dict):

    """
    A wrapper for an item returned from Dynamo

    Contains consumed capacity data

    """

    def __init__(self, dynamizer, response):
        super(Result, self).__init__()
        # For UpdateItem and PutItem
        for k, v in six.iteritems(response.get('Attributes', {})):
            self[k] = dynamizer.decode(v)
        # For GetItem
        for k, v in six.iteritems(response.get('Item', {})):
            self[k] = dynamizer.decode(v)
        cap = response.get('ConsumedCapacity', {})
        self.capacity = cap.get('CapacityUnits', 0)
        self.table_capacity = cap.get('Table', {}).get('CapacityUnits', 0)
        self.indexes = dict((
            (k, v['CapacityUnits']) for k, v in
            six.iteritems(cap.get('LocalSecondaryIndexes', {}))
        ))
        self.global_indexes = dict((
            (k, v['CapacityUnits']) for k, v in
            six.iteritems(cap.get('GlobalSecondaryIndexes', {}))
        ))

    def __repr__(self):
        return 'Result({0})'.format(super(Result, self).__repr__())


class DynamoDBConnection(object):

    """
    Connection to DynamoDB.

    You should generally call :meth:`~.connect_to_region` and
    :meth:`~.connect_to_host` instead of the constructor.

    Parameters
    ----------
    service : :class:`~botocore.service.Service`, optional
        The botocore service that will be used for requests
    endpoint : :class:`~botocore.endpoint.Endpoint`, optional
        The botocore endpoint that will be used for requests
    dynamizer : :class:`~dynamo3.types.Dynamizer`, optional
        The Dynamizer object to use for encoding/decoding values

    """

    def __init__(self, service=None, endpoint=None, dynamizer=Dynamizer()):
        self.service = service
        self.endpoint = endpoint
        self.dynamizer = dynamizer

    @property
    def host(self):
        """ The address of the endpoint """
        return self.endpoint.host

    @property
    def region(self):
        """ The name of the current connected region """
        return self.endpoint.region_name

    @classmethod
    def connect_to_region(cls, region, session=None, **kwargs):
        """
        Connect to an AWS region.

        Parameters
        ----------
        region : str
            Name of an AWS region or 'local'
        session : :class:`~botocore.session.Session`, optional
            The Session object to use for the connection
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        if session is None:
            session = botocore.session.get_session()
        service = session.get_service('dynamodb')
        return cls(service, service.get_endpoint(region), **kwargs)

    @classmethod
    def connect_to_host(cls, host='localhost', port=8000, is_secure=False,
                        session=None, **kwargs):
        """
        Connect to a specific host.

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
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        if session is None:
            session = botocore.session.get_session()
        url = "http://%s:%d" % (host, port)
        service = session.get_service('dynamodb')
        endpoint = service.get_endpoint(
            'local', endpoint_url=url, is_secure=is_secure)
        return cls(service, endpoint, **kwargs)

    def call(self, command, **kwargs):
        """
        Make a request to DynamoDB using the raw botocore API

        Raises
        ------
        exc : :class:`~.DynamoDBError`

        Returns
        -------
        data : dict

        """
        op = self.service.get_operation(command)
        response, data = op.call(self.endpoint, **kwargs)
        if 'Errors' in data:
            error = data['Errors'][0]
            error.setdefault('Message', '')
            err_class = EXC.get(error['Code'], DynamoDBError)
            raise err_class(response.status_code, args=pformat(kwargs),
                            **error)
        response.raise_for_status()
        return data

    def list_tables(self, limit=100):
        """
        List all tables.

        Parameters
        ----------
        limit : int, optional
            Maximum number of tables to return (default 100)

        Returns
        -------
        tables : Iterator
            Iterator that returns table names as strings

        """
        return ResultSet(self, 'TableNames', 'ListTables', limit=limit)

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
                'DescribeTable', table_name=tablename)['Table']
            return Table.from_response(response)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return None
            else:
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

        kwargs = {}
        if indexes:
            kwargs['local_secondary_indexes'] = [
                idx.schema(hash_key) for idx in indexes
            ]
            for idx in indexes:
                all_attrs.add(idx.range_key)

        if global_indexes:
            kwargs['global_secondary_indexes'] = [
                idx.schema() for idx in global_indexes
            ]
            for idx in global_indexes:
                all_attrs.add(idx.hash_key)
                if idx.range_key is not None:
                    all_attrs.add(idx.range_key)

        attr_definitions = [attr.definition() for attr in all_attrs]
        return self.call('CreateTable', table_name=tablename,
                         attribute_definitions=attr_definitions,
                         key_schema=key_schema,
                         provisioned_throughput=throughput.schema(),
                         **kwargs)

    def delete_table(self, tablename):
        """
        Delete a table

        Parameters
        ----------
        tablename : str
            Name of the table to delete

        Returns
        -------
        response : :class:`~dynamo3.fields.Table` or bool
            A Table if the table was deleted, False if no table exists

        """
        try:
            ret = self.call('DeleteTable', table_name=tablename)
            return Table.from_response(ret['TableDescription'])
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise

    def put_item(self, tablename, item, expected=None, returns=NONE,
                 return_capacity=NONE):
        """
        Store an item, overwriting existing data

        Parameters
        ----------
        tablename : str
            Name of the table to write
        item : dict
            Item data
        expected : dict, optional
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

        """
        kwargs = {}
        if expected is not None:
            kwargs['expected'] = build_expected(self.dynamizer, expected)
        item = self.dynamizer.encode_keys(item)
        ret = self.call('PutItem', table_name=tablename, item=item,
                        return_values=returns,
                        return_consumed_capacity=return_capacity, **kwargs)
        if ret:
            return Result(self.dynamizer, ret)

    def get_item(self, tablename, key, attributes=None, consistent=False,
                 return_capacity=NONE):
        """
        Fetch a single item from a table

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
        kwargs = {}
        if attributes is not None:
            kwargs['attributes_to_get'] = attributes
        key = self.dynamizer.encode_keys(key)
        return Result(self.dynamizer,
                      self.call('GetItem', table_name=tablename,
                                key=key,
                                consistent_read=consistent,
                                return_consumed_capacity=return_capacity,
                                **kwargs
                                )
                      )

    def delete_item(self, tablename, key, expected=None, returns=NONE,
                    return_capacity=NONE):
        """
        Delete an item

        Parameters
        ----------
        tablename : str
            Name of the table to delete from
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        expected : dict, optional
            If present, will check the values in Dynamo before performing the
            write. If values do not match, will raise an exception. (Using None
            as a value checks that the field does not exist).
        returns : {NONE, ALL_OLD}, optional
            If ALL_OLD, return the data that was deleted (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        key = self.dynamizer.encode_keys(key)
        kwargs = {}
        if expected is not None:
            kwargs['expected'] = build_expected(self.dynamizer, expected)
        ret = self.call('DeleteItem', table_name=tablename, key=key,
                        return_values=returns,
                        return_consumed_capacity=return_capacity,
                        **kwargs)
        if ret:
            return Result(self.dynamizer, ret)

    def batch_write(self, tablename):
        """
        Perform a batch write on a table

        Parameters
        ----------
        tablename : str
            Name of the table to write to

        Examples
        --------
        .. code-block:: python

            with connection.batch_write('mytable') as batch:
                batch.put({'id': 'id1', 'foo': 'bar'})
                batch.delete({'id': 'oldid'})

        """
        return BatchWriter(self, tablename)

    def batch_get(self, tablename, keys, attributes=None, consistent=False,
                  return_capacity=NONE):
        """
        Perform a batch get of many items in a table

        Parameters
        ----------
        tablename : str
            Name of the table to fetch from
        keys : list
            List of primary key dicts that specify the hash key and the
            optional range key of each item to fetch
        attributes : list, optional
            If present, only fetch these attributes from the item
        consistent : bool, optional
            Perform a strongly consistent read of the data (default False)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        kwargs = {}
        if attributes is not None:
            kwargs['attributes_to_get'] = attributes
        return GetResultSet(self, tablename, keys,
                            consistent=consistent, attributes=attributes,
                            return_capacity=return_capacity)

    def update_item(self, tablename, key, updates, returns=NONE,
                    return_capacity=NONE):
        """
        Update a single item in a table

        Parameters
        ----------
        tablename : str
            Name of the table to update
        key : dict
            Primary key dict specifying the hash key and, if applicable, the
            range key of the item.
        updates : list
            List of :class:`~.ItemUpdate`
        returns : {NONE, ALL_OLD, UPDATED_OLD, ALL_NEW, UPDATED_NEW}, optional
            Return either the old or new values, either all attributes or just
            the ones that changed. (default NONE)
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)

        """
        key = self.dynamizer.encode_keys(key)
        attr_updates = {}
        expected = {}
        for update in updates:
            attr_updates.update(update.attrs(self.dynamizer))
            expected.update(update.expected(self.dynamizer))
        kwargs = {}
        if expected:
            kwargs['expected'] = expected

        result = self.call('UpdateItem', table_name=tablename, key=key,
                           attribute_updates=attr_updates,
                           return_values=returns,
                           return_consumed_capacity=return_capacity,
                           **kwargs)
        if result:
            return Result(self.dynamizer, result)

    def scan(self, tablename, attributes=None, count=False, limit=None,
             **kwargs):
        """
        Perform a full-table scan

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
        keywords = {'table_name': tablename}
        if attributes is not None:
            keywords['attributes_to_get'] = attributes
        if limit is not None:
            keywords['limit'] = limit
        if kwargs:
            keywords['scan_filter'] = encode_query_kwargs(
                self.dynamizer, kwargs)
        if count:
            keywords['select'] = 'COUNT'
            return self.call('Scan', **keywords)['Count']
        else:
            return ResultSet(self, 'Items', 'Scan', **keywords)

    def query(self, tablename, attributes=None, consistent=None, count=False,
              index=None, limit=None, desc=False, **kwargs):
        """
        Perform an index query on a table

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
        keywords = {'table_name': tablename}
        if attributes is not None:
            keywords['attributes_to_get'] = attributes
        if consistent is not None:
            keywords['consistent_read'] = consistent
        if index is not None:
            keywords['index_name'] = index
        if limit is not None:
            keywords['limit'] = limit
        keywords['scan_index_forward'] = not desc

        keywords['key_conditions'] = encode_query_kwargs(self.dynamizer,
                                                         kwargs)
        if count:
            keywords['select'] = 'COUNT'
            return self.call('Query', **keywords)['Count']
        else:
            return ResultSet(self, 'Items', 'Query', **keywords)

    def update_table(self, tablename, throughput=None, global_indexes=None):
        """
        Update the throughput of a table and/or global indexes

        Parameters
        ----------
        tablename : str
            Name of the table to update
        throughput : :class:`~dynamo3.fields.Throughput`, optional
            The new throughput of the table
        global_indexes : dict, optional
            Map of index name to :class:`~dynamo3.fields.Throughput`

        """
        kwargs = {}
        if throughput is not None:
            kwargs['provisioned_throughput'] = throughput.schema()
        if global_indexes is not None:
            kwargs['global_secondary_index_updates'] = [
                {
                    'Update': {
                        'IndexName': key,
                        'ProvisionedThroughput': value.schema(),
                    }
                }
                for key, value in six.iteritems(global_indexes)
            ]
        return self.call('UpdateTable', table_name=tablename, **kwargs)
