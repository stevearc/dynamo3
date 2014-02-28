""" Wrappers for result objects and iterators """
import six
from six.moves import xrange as _xrange  # pylint: disable=F0401

from .constants import NONE


class PagedIterator(six.Iterator):

    """ An iterator that iterates over paged results from Dynamo """

    def __init__(self):
        self.iterator = None
        self.capacity = 0
        self.table_capacity = 0
        self.indexes = {}
        self.global_indexes = {}

    @property
    def can_fetch_more(self):
        """ Return True if more results can be fetched from the server """
        return True

    def fetch(self):  # pragma: no cover
        """ Fetch additional results from the server and return an iterator """
        raise NotImplementedError

    def _update_capacity(self, data):
        """ Update the consumed capacity metrics """
        cap = data.get('ConsumedCapacity', {})
        self.capacity += cap.get('CapacityUnits', 0)
        self.table_capacity += cap.get('Table', {}).get('CapacityUnits', 0)
        for k, v in six.iteritems(cap.get('LocalSecondaryIndexes', {})):
            self.indexes.setdefault(k, 0)
            self.indexes[k] += v['CapacityUnits']
        for k, v in six.iteritems(cap.get('GlobalSecondaryIndexes', {})):
            self.global_indexes.setdefault(k, 0)
            self.global_indexes[k] += v['CapacityUnits']

    def __iter__(self):
        return self

    def __next__(self):
        if self.iterator is None:
            self.iterator = self.fetch()
        try:
            return six.next(self.iterator)
        except StopIteration:
            if self.can_fetch_more:
                self.iterator = self.fetch()
            return six.next(self.iterator)


class ResultSet(PagedIterator):

    """ Iterator that pages results from Dynamo """

    def __init__(self, connection, response_key=None, *args, **kwargs):
        super(ResultSet, self).__init__()
        self.connection = connection
        self.response_key = response_key
        self.args = args
        self.kwargs = kwargs
        self.last_evaluated_key = None

    @property
    def can_fetch_more(self):
        """ True if there are more results on the server """
        return (self.last_evaluated_key is not None and
                self.kwargs.get('limit') != 0)

    def fetch(self):
        """ Fetch more results from Dynamo """
        data = self.connection.call(*self.args, **self.kwargs)
        self.last_evaluated_key = data.get('LastEvaluatedKey')
        if self.last_evaluated_key is not None:
            self.kwargs['exclusive_start_key'] = self.last_evaluated_key
        else:
            self.kwargs.pop('exclusive_start_key', None)
        self._update_capacity(data)
        return iter(data[self.response_key])

    def __next__(self):
        result = super(ResultSet, self).__next__()
        if 'limit' in self.kwargs:
            self.kwargs['limit'] -= 1
        if isinstance(result, dict):
            return self.connection.dynamizer.decode_keys(result)
        return result


class GetResultSet(PagedIterator):

    """ Iterator that pages the results of a BatchGetItem """

    def __init__(self, connection, tablename, keys, consistent=False,
                 attributes=None, return_capacity=NONE):
        super(GetResultSet, self).__init__()
        self.connection = connection
        self.tablename = tablename
        self.key_iter = iter(keys)
        self.consistent = consistent
        self.attributes = attributes
        self.return_capacity = return_capacity
        self.page_size = 100
        self.unprocessed_keys = []

    def _get_next_keys(self):
        """ Get the next page of keys to fetch """
        keys = []
        try:
            # First try to iterate through the keys we were given
            for _ in _xrange(self.page_size):
                key = six.next(self.key_iter)
                keys.append(self.connection.dynamizer.encode_keys(key))
        except StopIteration:
            # If there are no keys left, check the unprocessed keys
            if not keys:
                if self.unprocessed_keys:
                    keys = self.unprocessed_keys[:self.page_size]
                    self.unprocessed_keys = \
                        self.unprocessed_keys[self.page_size:]
                else:
                    # If we're out of unprocessed keys, we're out of all keys
                    raise StopIteration
        return keys

    def fetch(self):
        """ Fetch a set of items from their keys """
        keys = self._get_next_keys()
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
        for items in six.itervalues(data.get('UnprocessedKeys', {})):
            self.unprocessed_keys.extend(items['Keys'])
        self._update_capacity(data)
        return iter(data['Responses'][self.tablename])

    def __next__(self):
        result = super(GetResultSet, self).__next__()
        return self.connection.dynamizer.decode_keys(result)


class Result(dict):

    """
    A wrapper for an item returned from Dynamo

    Attributes
    ----------
    capacity : float
        Total consumed capacity
    table_capacity : float
        Consumed capacity on the table
    indexes : dict
        Mapping of local index name to the consumed capacity on that index
    global_indexes : dict
        Mapping of global index name to the consumed capacity on that index

    """

    def __init__(self, dynamizer, response, item_key):
        super(Result, self).__init__()
        for k, v in six.iteritems(response.get(item_key, {})):
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
