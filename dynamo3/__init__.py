# TODO: retries on ProvisionedThroughputExceededException
# TODO: results should log consumed capacity
from pprint import pformat
import botocore.session
from decimal import Decimal
import base64
import logging
import six
from .fields import (DynamoKey, AllIndex, KeysOnlyIndex, IncludeIndex,
                     GlobalAllIndex, GlobalKeysOnlyIndex,
                     GlobalIncludeIndex, Throughput, Table)
from .types import (STRING, NUMBER, BINARY, STRING_SET, NUMBER_SET, BINARY_SET,
                    TYPES, TYPES_REV, Binary, Dynamizer)

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
    fmt = '{Code}: {Message}\nArgs: {args}'

    def __init__(self, status_code, **kwargs):
        self.status_code = status_code
        super(DynamoDBError, self).__init__(**kwargs)


class ConditionalCheckFailedException(DynamoDBError):
    fmt = '{Code}: {Message}'

    def __init__(self, status_code, **kwargs):
        super(ConditionalCheckFailedException, self).__init__(status_code,
                                                              **kwargs)

CheckFailed = ConditionalCheckFailedException

EXC = {
    'ConditionalCheckFailedException': ConditionalCheckFailedException,
}


class BatchWriter(object):

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

    def put_item(self, data):
        self._to_put.append(data)

        if self.should_flush():
            self.flush()

    def delete_item(self, **kwargs):
        self._to_delete.append(kwargs)

        if self.should_flush():
            self.flush()

    def should_flush(self):
        return len(self._to_put) + len(self._to_delete) == 25

    def flush(self):
        items = []

        for data in self._to_put:
            items.append(ItemPut(data))

        for data in self._to_delete:
            items.append(ItemDelete(data))

        resp = self.connection.batch_write_item(self.tablename, items)
        self.handle_unprocessed(resp)

        self._to_put = []
        self._to_delete = []
        return True

    def handle_unprocessed(self, resp):
        if len(resp.get('UnprocessedItems', [])):
            unprocessed = resp['UnprocessedItems'].get(self.tablename, [])

            # Some items have not been processed. Stow them for now &
            # re-attempt processing on ``__exit__``.
            LOG.info("%d items were unprocessed. Storing for later.",
                     len(unprocessed))
            self._unprocessed.extend(unprocessed)

    def resend_unprocessed(self):
        # If there are unprocessed records (for instance, the user was over
        # their throughput limitations), iterate over them & send until they're
        # all there.
        LOG.info("Re-sending %s unprocessed items.", len(self._unprocessed))

        while len(self._unprocessed):
            # Again, do 25 at a time.
            to_resend = self._unprocessed[:25]
            # Remove them from the list.
            self._unprocessed = self._unprocessed[25:]
            LOG.info("Sending %s items", len(to_resend))
            resp = self.connection.batch_write_item(self.tablename, to_resend)
            self.handle_unprocessed(resp)
            LOG.info("%s unprocessed items left", len(self._unprocessed))


def is_null(value):
    return value is None or value == set() or value == frozenset()


class ItemWrite(object):

    def __init__(self, data, action, key):
        self.data = data
        self.action = action
        self.key = key

    def encode(self, dynamizer):
        # Strip None values out of data
        data = dict(((k, v) for k, v in six.iteritems(self.data)
                     if not is_null(v)))
        return {
            self.action: {
                self.key: dynamizer.encode_keys(data)
            }
        }


class ItemPut(ItemWrite):

    def __init__(self, data):
        super(ItemPut, self).__init__(data, 'PutRequest', 'Item')


class ItemDelete(ItemWrite):

    def __init__(self, data):
        super(ItemDelete, self).__init__(data, 'DeleteRequest', 'Key')

NO_ARG = object()


class ItemUpdate(object):
    ADD = 'ADD'
    DELETE = 'DELETE'
    PUT = 'PUT'

    def __init__(self, action, key, value=None, expect_value=NO_ARG):
        if value is None and action != self.DELETE:
            raise ValueError(
                "Update must set a value for non-delete operations!")
        self.action = action
        self.key = key
        self.value = value
        self.expect_value = expect_value

    @classmethod
    def put(cls, *args, **kwargs):
        return cls(cls.PUT, *args, **kwargs)

    @classmethod
    def add(cls, *args, **kwargs):
        return cls(cls.ADD, *args, **kwargs)

    @classmethod
    def delete(cls, *args, **kwargs):
        return cls(cls.DELETE, *args, **kwargs)

    def attrs(self, dynamizer):
        ret = {
            self.key: {
                'Action': self.action,
            }
        }
        if self.value is not None:
            ret[self.key]['Value'] = dynamizer.encode(self.value)
        return ret

    def expected(self, dynamizer):
        if self.expect_value is not NO_ARG:
            ret = {}
            if self.expect_value is None:
                ret['Exists'] = False
            else:
                ret['Value'] = dynamizer.encode(self.expect_value)
                ret['Exists'] = True
            return {self.key: ret}
        return {}


class ResultSet(object):

    def __init__(self, connection, *args, **kwargs):
        self.connection = connection
        self.args = args
        self.kwargs = kwargs
        self.results = None
        self.last_evaluated_key = None
        self.fetch()

    @property
    def can_fetch_more(self):
        """ True if there are more results on the server """
        return self.last_evaluated_key is not None and self.kwargs.get('limit', -1) != 0

    def fetch(self):
        data = self.connection.call(*self.args, **self.kwargs)
        self.results = data['Items']
        self.last_evaluated_key = data.get('LastEvaluatedKey')
        if self.last_evaluated_key is not None:
            self.kwargs['exclusive_start_key'] = self.last_evaluated_key
        elif 'exclusive_start_key' in self.kwargs:
            del self.kwargs['exclusive_start_key']

    def __iter__(self):
        while self.results is not None:
            for r in self.results:
                yield dict(((k, self.connection.dynamizer.decode(v)) for k, v
                            in six.iteritems(r)))
            if 'limit' in self.kwargs:
                self.kwargs['limit'] -= len(self.results)
            self.results = None
            if self.can_fetch_more:
                self.fetch()


class GetResultSet(ResultSet):

    def __init__(self, connection, tablename, keys, consistent, attributes):
        self.connection = connection
        self.tablename = tablename
        self.keys = keys
        self.consistent = consistent
        self.attributes = attributes
        self.page_size = 100
        self.unprocessed_keys = []

    def fetch_items(self, keys):
        query = {'ConsistentRead': self.consistent}
        if self.attributes is not None:
            query['AttributesToGet'] = self.attributes
        query['Keys'] = keys
        kwargs = {
            'request_items': {
                self.tablename: query,
            },
        }
        data = self.connection.call('BatchGetItem', **kwargs)
        for items in six.itervalues(data['UnprocessedKeys']):
            self.unprocessed_keys.extend(items['Keys'])
        for items in six.itervalues(data['Responses']):
            for item in items:
                yield dict(((k, self.connection.dynamizer.decode(v)) for k,
                            v in six.iteritems(item)))

    def __iter__(self):
        for i in six.moves.range(0, len(self.keys), self.page_size):
            keys = []
            for key in self.keys[i:i+self.page_size]:
                keys.append(dict(((k, self.connection.dynamizer.encode(v)) for
                                  k, v in six.iteritems(key))))
            for item in self.fetch_items(keys):
                yield item
        while self.unprocessed_keys:
            keys = self.unprocessed_keys[:self.page_size]
            self.unprocessed_keys = self.unprocessed_keys[self.page_size:]
            for key in self.fetch_items(keys):
                yield key


class Result(dict):

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

    def __init__(self, service=None, endpoint=None, dynamizer=Dynamizer()):
        self.service = service
        self.endpoint = endpoint
        self.dynamizer = dynamizer

    @property
    def host(self):
        return self.endpoint.host

    @property
    def region(self):
        return self.endpoint.region_name

    @classmethod
    def connect_to_region(cls, region, session=None, **kwargs):
        """
        Connect to an AWS region.

        Parameters
        ----------
        region : str
            Name of an AWS region or 'local'

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

        """
        if session is None:
            session = botocore.session.get_session()
        url = "http://%s:%d" % (host, port)
        service = session.get_service('dynamodb')
        endpoint = service.get_endpoint(
            'local', endpoint_url=url, is_secure=is_secure)
        return cls(service, endpoint, **kwargs)

    def call(self, command, **kwargs):
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

    def list_tables(self):
        return self.call('ListTables')['TableNames']

    def describe_table(self, tablename, raw=False):
        try:
            response = self.call(
                'DescribeTable', table_name=tablename)['Table']
            if raw:
                return response
            else:
                return Table.from_response(response)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return None
            else:
                raise

    def create_table(self, tablename, hash_key, range_key=None,
                     indexes=None, global_indexes=None, throughput=None):

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
                         key_schema=key_schema, provisioned_throughput=throughput.schema(
                             ),
                         **kwargs)

    def delete_table(self, tablename):
        try:
            return self.call('DeleteTable', table_name=tablename)
        except DynamoDBError as e:
            if e.kwargs['Code'] == 'ResourceNotFoundException':
                return False
            else:
                raise

    def put_item(self, tablename, item, expected=None, returns='NONE'):
        kwargs = {}
        if expected is not None:
            pass
        item = self.dynamizer.encode_keys(item)
        return self.call('PutItem', table_name=tablename, item=item,
                         return_values=returns, **kwargs)

    def get_item(self, tablename, key, attributes=None, consistent=False,
                 return_capacity=NONE):
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

    def batch_write(self, tablename):
        return BatchWriter(self, tablename)

    def batch_write_item(self, tablename, items):
        data = {
            tablename: [
                item.encode(self.dynamizer) for item in items
            ]
        }

        return self.call('BatchWriteItem', request_items=data)

    def batch_get(self, tablename, keys, attributes=None, consistent=False):
        kwargs = {}
        if attributes is not None:
            kwargs['attributes_to_get'] = attributes
        return GetResultSet(self, tablename, keys,
                            consistent=consistent, attributes=attributes)

    def update_item(self, tablename, key, updates, returns=NONE,
                    return_capacity=NONE):
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

    def scan(self, tablename, attributes=None, count=False, limit=None, **kwargs):
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
            return ResultSet(self, 'Scan', **keywords)

    def query(self, tablename, attributes=None, consistent=None, count=False,
              index=None, limit=None, desc=False, **kwargs):
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
            return ResultSet(self, 'Query', **keywords)

    def update_table(self, tablename, throughput=None, global_indexes=None):
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
