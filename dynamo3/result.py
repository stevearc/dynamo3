""" Wrappers for result objects and iterators """
import six

from .constants import NONE, MAX_GET_BATCH


class PagedIterator(six.Iterator):

    """ An iterator that iterates over paged results from Dynamo """

    def __init__(self):
        self.iterator = None
        self.capacity = 0
        self.table_capacity = 0
        self.indexes = {}
        self.global_indexes = {}

    @property
    def can_fetch_more(self):  # pragma: no cover
        """ Return True if more results can be fetched from the server """
        raise NotImplementedError

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
                self.kwargs.get('Limit') != 0)

    def fetch(self):
        """ Fetch more results from Dynamo """
        data = self.connection.call(*self.args, **self.kwargs)
        self.last_evaluated_key = data.get('LastEvaluatedKey')
        if self.last_evaluated_key is not None:
            self.kwargs['ExclusiveStartKey'] = self.last_evaluated_key
        else:
            self.kwargs.pop('ExclusiveStartKey', None)
        self._update_capacity(data)
        return iter(data[self.response_key])

    def __next__(self):
        result = super(ResultSet, self).__next__()
        if 'Limit' in self.kwargs:
            self.kwargs['Limit'] -= 1
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
        self.keys = keys
        self.consistent = consistent
        self.attributes = attributes
        self.return_capacity = return_capacity
        self._attempt = 0

    @property
    def can_fetch_more(self):
        return bool(self.keys)

    def build_kwargs(self):
        """ Construct the kwargs to pass to batch_get_item """
        keys, self.keys = self.keys[:MAX_GET_BATCH], self.keys[MAX_GET_BATCH:]
        query = {'ConsistentRead': self.consistent}
        if self.attributes is not None:
            query['AttributesToGet'] = self.attributes
        query['Keys'] = keys
        return {
            'RequestItems': {
                self.tablename: query,
            },
            'ReturnConsumedCapacity': self.return_capacity,
        }

    def fetch(self):
        """ Fetch a set of items from their keys """
        kwargs = self.build_kwargs()
        data = self.connection.call('batch_get_item', **kwargs)
        if 'UnprocessedKeys' in data:
            for items in six.itervalues(data['UnprocessedKeys']):
                self.keys.extend(items['Keys'])
            # Getting UnprocessedKeys indicates that we are exceeding our
            # throughput. So sleep for a bit.
            self._attempt += 1
            self.connection.exponential_sleep(self._attempt)
        else:
            # No UnprocessedKeys means our request rate is fine, so we can
            # reset the attempt number.
            self._attempt = 0
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
