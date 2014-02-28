""" Wrappers for result objects and iterators """
import six
from six.moves import xrange as _xrange  # pylint: disable=F0401

from .constants import NONE


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
