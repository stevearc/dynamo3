""" Wrappers for result objects and iterators """
import six

from .constants import NONE, MAX_GET_BATCH
from .util import snake_to_camel


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

    def __new__(cls, count, response=None):
        ret = super(Count, cls).__new__(cls, count)
        ret.response = response or {}
        ret.consumed_capacity = ret.response.get('consumed_capacity')
        return ret

    @classmethod
    def from_response(cls, response):
        """ Factory method """
        return cls(response['Count'], response)

    def __getitem__(self, name):
        return self.response[name]

    def __getattr__(self, name):
        camel_name = snake_to_camel(name)
        if camel_name in self.response:
            return self.response[camel_name]
        return super(Count, self).__getattribute__(name)

    def __repr__(self):
        return "Count(%s)" % self


@six.python_2_unicode_compatible
class Capacity(object):
    """ Wrapper for the capacity of a table or index """

    def __init__(self, read, write):
        self._read = read
        self._write = write

    @property
    def read(self):
        """ The read capacity """
        return self._read

    @property
    def write(self):
        """ The write capacity """
        return self._write

    @classmethod
    def create_read(cls, value):
        """ Create a new read capacity from a response """
        return cls(value['CapacityUnits'], 0)

    @classmethod
    def create_write(cls, value):
        """ Create a new write capacity from a response """
        return cls(0, value['CapacityUnits'])

    def __hash__(self):
        return self._read + self._write

    def __eq__(self, other):
        return (self.read == getattr(other, 'read', None) and
                self.write == getattr(other, 'write', None))

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        return Capacity(self.read + other.read, self.write + other.write)

    def __str__(self):
        pieces = []
        if self.read:
            pieces.append('R:{0:.1f}'.format(self.read))
        if self.write:
            pieces.append('W:{0:.1f}'.format(self.write))
        if not pieces:
            return '0'
        return ' '.join(pieces)


@six.python_2_unicode_compatible
class ConsumedCapacity(object):
    """ Record of the consumed capacity of a request """

    def __init__(self, tablename, total, table_capacity=None,
                 local_index_capacity=None, global_index_capacity=None):
        self.tablename = tablename
        self.total = total
        self.table_capacity = table_capacity
        self.local_index_capacity = local_index_capacity
        self.global_index_capacity = global_index_capacity

    @classmethod
    def build_indexes(cls, response, key, capacity_factory):
        """ Construct index capacity map from a request fragment """
        if key not in response:
            return None
        indexes = {}
        for key, val in six.iteritems(response[key]):
            indexes[key] = capacity_factory(val)
        return indexes

    @classmethod
    def from_response(cls, response, is_read):
        """ Factory method for ConsumedCapacity from a response object """
        if is_read:
            cap = Capacity.create_read
        else:
            cap = Capacity.create_write
        kwargs = {
            'tablename': response['TableName'],
            'total': cap(response),
        }
        local = cls.build_indexes(response, 'LocalSecondaryIndexes', cap)
        kwargs['local_index_capacity'] = local
        gindex = cls.build_indexes(response, 'GlobalSecondaryIndexes', cap)
        kwargs['global_index_capacity'] = gindex
        if 'Table' in response:
            kwargs['table_capacity'] = cap(response['Table'])
        return cls(**kwargs)

    def __hash__(self):
        return hash(self.tablename) + hash(self.total)

    def __eq__(self, other):
        properties = ['tablename', 'total', 'table_capacity',
                      'local_index_capacity', 'global_index_capacity']
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
            'total': self.total + other.total,
        }
        if self.table_capacity is not None:
            kwargs['table_capacity'] = (self.table_capacity +
                                        other.table_capacity)
        kwargs['local_index_capacity'] = \
            add_dicts(self.local_index_capacity, other.local_index_capacity)
        kwargs['global_index_capacity'] = \
            add_dicts(self.global_index_capacity, other.global_index_capacity)

        return ConsumedCapacity(self.tablename, **kwargs)

    def __str__(self):
        lines = []
        if self.table_capacity:
            lines.append("Table: %s" % self.table_capacity)
        if self.local_index_capacity:
            for name, cap in six.iteritems(self.local_index_capacity):
                lines.append("Local index '%s': %s" % (name, cap))
        if self.global_index_capacity:
            for name, cap in six.iteritems(self.global_index_capacity):
                lines.append("Global index '%s': %s" % (name, cap))
        lines.append("Total: %s" % self.total)
        return '\n'.join(lines)


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
        if 'ConsumedCapacity' in data:
            # This is all for backwards compatibility
            consumed = data['ConsumedCapacity']
            if not isinstance(consumed, list):
                consumed = [consumed]
            for cap in consumed:
                self.capacity += cap.get('CapacityUnits', 0)
                self.table_capacity += cap.get('Table',
                                               {}).get('CapacityUnits', 0)
                local_indexes = cap.get('LocalSecondaryIndexes', {})
                for k, v in six.iteritems(local_indexes):
                    self.indexes.setdefault(k, 0)
                    self.indexes[k] += v['CapacityUnits']
                global_indexes = cap.get('GlobalSecondaryIndexes', {})
                for k, v in six.iteritems(global_indexes):
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
        self.consumed_capacity = None

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
        if 'consumed_capacity' in data:
            self.consumed_capacity += data['consumed_capacity']
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
                 attributes=None, alias=None, return_capacity=NONE):
        super(GetResultSet, self).__init__()
        self.connection = connection
        self.tablename = tablename
        self.keys = keys
        self.consistent = consistent
        if attributes is not None:
            if not isinstance(attributes, six.string_types):
                attributes = ', '.join(attributes)
        self.attributes = attributes
        self.alias = alias
        self.return_capacity = return_capacity
        self._attempt = 0
        self.consumed_capacity = None

    @property
    def can_fetch_more(self):
        return bool(self.keys)

    def build_kwargs(self):
        """ Construct the kwargs to pass to batch_get_item """
        keys, self.keys = self.keys[:MAX_GET_BATCH], self.keys[MAX_GET_BATCH:]
        query = {'ConsistentRead': self.consistent}
        if self.attributes is not None:
            query['ProjectionExpression'] = self.attributes
        if self.alias:
            query['ExpressionAttributeNames'] = self.alias
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
        if 'consumed_capacity' in data:
            # Comes back as a list from BatchWriteItem
            self.consumed_capacity = \
                sum(data['consumed_capacity'], self.consumed_capacity)
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

        self.consumed_capacity = response.get('consumed_capacity')

        # All this shit is for backwards compatibility
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
