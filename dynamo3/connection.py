""" Connection class for DynamoDB """
import time

import botocore.session
import six

from .batch import BatchWriter
from .constants import NONE
from .exception import raise_if_error, DynamoDBError, ThroughputException
from .fields import Throughput, Table
from .result import ResultSet, GetResultSet, Result
from .types import Dynamizer
from .util import is_null


CONDITIONS = {
    'eq': 'EQ',
    'ne': 'NE',
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
        elif not isinstance(v, (list, tuple, set, frozenset)):
            v = (v,)
        ret[name] = {
            'AttributeValueList': [dynamizer.encode(value) for value in v],
            'ComparisonOperator': CONDITIONS[condition_key],
        }
    return ret


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

    Attributes
    ----------
    request_retries : int
        Number of times to retry an API call if the throughput is exceeded
        (default 10)

    """

    def __init__(self, service=None, endpoint=None, dynamizer=Dynamizer()):
        self.service = service
        self.endpoint = endpoint
        self.dynamizer = dynamizer
        self.request_retries = 10

    @property
    def host(self):
        """ The address of the endpoint """
        return self.endpoint.host

    @property
    def region(self):
        """ The name of the current connected region """
        return self.endpoint.region_name

    @classmethod
    def connect_to_region(cls, region, session=None, access_key=None,
                          secret_key=None, **kwargs):
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
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        if session is None:
            session = botocore.session.get_session()
            if access_key is not None:
                session.set_credentials(access_key, secret_key)
        service = session.get_service('dynamodb')
        return cls(service, service.get_endpoint(region), **kwargs)

    @classmethod
    def connect_to_host(cls, host='localhost', port=8000, is_secure=False,
                        session=None, access_key=None, secret_key=None,
                        **kwargs):
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
        access_key : str, optional
            If session is None, set this access key when creating the session
        secret_key : str, optional
            If session is None, set this secret key when creating the session
        **kwargs : dict
            Keyword arguments to pass to the constructor

        """
        if session is None:
            session = botocore.session.get_session()
            if access_key is not None:
                session.set_credentials(access_key, secret_key)
        url = "http://%s:%d" % (host, port)
        service = session.get_service('dynamodb')
        endpoint = service.get_endpoint(
            'local', endpoint_url=url, is_secure=is_secure)
        return cls(service, endpoint, **kwargs)

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
        op = self.service.get_operation(command)
        attempt = 0
        while True:
            response, data = op.call(self.endpoint, **kwargs)
            try:
                raise_if_error(kwargs, response, data)
                break
            except ThroughputException:
                attempt += 1
                if attempt > self.request_retries:
                    raise
                self.exponential_sleep(attempt)
        return data

    def exponential_sleep(self, attempt):
        """ Sleep with exponential backoff """
        if attempt > 1:
            time.sleep(0.1 * 2 ** attempt)

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
        response : bool
            True if the table was deleted, False if no table exists

        """
        try:
            self.call('DeleteTable', table_name=tablename)
            return True
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return False
            else:  # pragma: no cover
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
            return Result(self.dynamizer, ret, 'Attributes')

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
        data = self.call('GetItem', table_name=tablename, key=key,
                         consistent_read=consistent,
                         return_consumed_capacity=return_capacity, **kwargs)
        return Result(self.dynamizer, data, 'Item')

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
            return Result(self.dynamizer, ret, 'Attributes')

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
        keys : list or iterable
            List or iterable of primary key dicts that specify the hash key and
            the optional range key of each item to fetch
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
            List of :class:`~dynamo3.batch.ItemUpdate`
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
            return Result(self.dynamizer, result, 'Attributes')

    def scan(self, tablename, attributes=None, count=False, limit=None,
             return_capacity=NONE, **kwargs):
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
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
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
            'table_name': tablename,
            'return_consumed_capacity': return_capacity,
        }
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

    def query(self, tablename, attributes=None, consistent=False, count=False,
              index=None, limit=None, desc=False, return_capacity=NONE,
              **kwargs):
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
        return_capacity : {NONE, INDEXES, TOTAL}, optional
            INDEXES will return the consumed capacity for indexes, TOTAL will
            return the consumed capacity for the table and the indexes.
            (default NONE)
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
            'table_name': tablename,
            'return_consumed_capacity': return_capacity,
            'consistent_read': consistent,
        }
        if attributes is not None:
            keywords['attributes_to_get'] = attributes
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
