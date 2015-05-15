""" Connection class for DynamoDB """
import time
import warnings

import botocore.session
import logging
import six
from botocore.exceptions import ClientError

from .batch import BatchWriter, encode_query_kwargs
from .constants import NONE
from .exception import translate_exception, DynamoDBError, ThroughputException
from .fields import Throughput, Table
from .result import ResultSet, GetResultSet, Result
from .types import Dynamizer
from .util import is_null


LOG = logging.getLogger(__name__)


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
    client : :class:`~botocore.client.BaseClient`, optional
        The botocore client that will be used for requests
    dynamizer : :class:`~dynamo3.types.Dynamizer`, optional
        The Dynamizer object to use for encoding/decoding values

    Attributes
    ----------
    request_retries : int
        Number of times to retry an API call if the throughput is exceeded
        (default 10)

    """

    def __init__(self, client=None, dynamizer=Dynamizer()):
        self.client = client
        self.dynamizer = dynamizer
        self.request_retries = 10

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
        return ResultSet(self, 'TableNames', 'list_tables', Limit=limit)

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

        kwargs = {}
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

        attr_definitions = [attr.definition() for attr in all_attrs]
        return self.call('create_table', TableName=tablename,
                         AttributeDefinitions=attr_definitions,
                         KeySchema=key_schema,
                         ProvisionedThroughput=throughput.schema(),
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
            self.call('delete_table', TableName=tablename)
            return True
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return False
            else:  # pragma: no cover
                raise

    def put_item(self, tablename, item, expected=None, returns=NONE,
                 return_capacity=NONE, expect_or=False, **kwargs):
        """
        Store an item, overwriting existing data

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
            LOG.warn("Using deprecated argument 'expected' for put_item")
            keywords['Expected'] = build_expected(self.dynamizer, expected)
        item = self.dynamizer.encode_keys(item)
        ret = self.call('put_item', TableName=tablename, Item=item,
                        ReturnValues=returns,
                        ReturnConsumedCapacity=return_capacity, **keywords)
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
            kwargs['AttributesToGet'] = attributes
        key = self.dynamizer.encode_keys(key)
        data = self.call('get_item', TableName=tablename, Key=key,
                         ConsistentRead=consistent,
                         ReturnConsumedCapacity=return_capacity, **kwargs)
        return Result(self.dynamizer, data, 'Item')

    def delete_item(self, tablename, key, expected=None, returns=NONE,
                    return_capacity=NONE, expect_or=False, **kwargs):
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
        keywords = {}
        if kwargs:
            keywords['Expected'] = encode_query_kwargs(self.dynamizer, kwargs)
            if len(keywords['Expected']) > 1:
                keywords['ConditionalOperator'] = 'OR' if expect_or else 'AND'
        elif expected is not None:
            LOG.warn("Using deprecated argument 'expected' for delete_item")
            keywords['Expected'] = build_expected(self.dynamizer, expected)
        ret = self.call('delete_item', TableName=tablename, Key=key,
                        ReturnValues=returns,
                        ReturnConsumedCapacity=return_capacity,
                        **keywords)
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
        return GetResultSet(self, tablename, keys,
                            consistent=consistent, attributes=attributes,
                            return_capacity=return_capacity)

    def update_item(self, tablename, key, updates, returns=NONE,
                    return_capacity=NONE, expect_or=False, **kwargs):
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
        keywords = {}
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
                           ReturnConsumedCapacity=return_capacity,
                           **keywords)
        if result:
            return Result(self.dynamizer, result, 'Attributes')

    def scan(self, tablename, attributes=None, count=False, limit=None,
             return_capacity=NONE, filter_or=False, **kwargs):
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
        filter_or : bool, optional
            If True, multiple filter kwargs will be OR'd together. If False,
            they will be AND'd together. (default False)
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
            'ReturnConsumedCapacity': return_capacity,
        }
        if attributes is not None:
            keywords['AttributesToGet'] = attributes
        if limit is not None:
            keywords['Limit'] = limit
        if kwargs:
            keywords['ScanFilter'] = encode_query_kwargs(
                self.dynamizer, kwargs)
            if len(kwargs) > 1:
                keywords['ConditionalOperator'] = 'OR' if filter_or else 'AND'
        if count:
            keywords['Select'] = 'COUNT'
            return self.call('scan', **keywords)['Count']
        else:
            return ResultSet(self, 'Items', 'scan', **keywords)

    def query(self, tablename, attributes=None, consistent=False, count=False,
              index=None, limit=None, desc=False, return_capacity=NONE,
              filter=None, filter_or=False, **kwargs):
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
        filter : dict, optional
            Query arguments. Same format as **kwargs, but these arguments
            filter the results on the server before they are returned. They
            will NOT use an index, as that is what the **kwargs are for.
        filter_or : bool, optional
            If True, multiple filter args will be OR'd together. If False, they
            will be AND'd together. (default False)
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
            'ReturnConsumedCapacity': return_capacity,
            'ConsistentRead': consistent,
        }
        if attributes is not None:
            keywords['AttributesToGet'] = attributes
        if index is not None:
            keywords['IndexName'] = index
        if limit is not None:
            keywords['Limit'] = limit
        if filter is not None:
            if len(filter) > 1:
                keywords['ConditionalOperator'] = 'OR' if filter_or else 'AND'
            keywords['QueryFilter'] = encode_query_kwargs(self.dynamizer,
                                                          filter)

        keywords['ScanIndexForward'] = not desc

        keywords['KeyConditions'] = encode_query_kwargs(self.dynamizer,
                                                         kwargs)
        if count:
            keywords['Select'] = 'COUNT'
            return self.call('query', **keywords)['Count']
        else:
            return ResultSet(self, 'Items', 'query', **keywords)

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
            kwargs['ProvisionedThroughput'] = throughput.schema()
        if global_indexes is not None:
            kwargs['GlobalSecondaryIndexUpdates'] = [
                {
                    'Update': {
                        'IndexName': key,
                        'ProvisionedThroughput': value.schema(),
                    }
                }
                for key, value in six.iteritems(global_indexes)
            ]
        return self.call('update_table', TableName=tablename, **kwargs)
