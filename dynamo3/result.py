""" Wrappers for result objects and iterators """
import six

from .constants import NONE, MAX_GET_BATCH


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

    def __new__(cls, count, scanned_count, consumed_capacity=None):
        ret = super(Count, cls).__new__(cls, count)
        ret.count = count
        ret.scanned_count = scanned_count
        ret.consumed_capacity = consumed_capacity
        return ret

    @classmethod
    def from_response(cls, response):
        """ Factory method """
        return cls(response['Count'], response['ScannedCount'],
                   response.get('consumed_capacity'))

    def __add__(self, other):
        if other is None:
            return self
        if not isinstance(other, Count):
            return self.count + other
        if self.consumed_capacity is None:
            capacity = other.consumed_capacity
        else:
            capacity = self.consumed_capacity + other.consumed_capacity
        return Count(self.count + other.count, self.scanned_count +
                     other.scanned_count, capacity)

    def __radd__(self, other):
        return self.__add__(other)

    def __repr__(self):
        return "Count(%d)" % self


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

    def __getitem__(self, key):
        return getattr(self, key)

    def __contains__(self, key):
        return key in ['read', 'write']

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

    def __init__(self, connection, limit, *args, **kwargs):
        super(ResultSet, self).__init__()
        self.connection = connection
        # The limit will be mutated, so copy it and leave the original intact
        self.limit = limit.copy()
        self.args = args
        self.kwargs = kwargs
        self.last_evaluated_key = None
        self.consumed_capacity = None

    @property
    def can_fetch_more(self):
        """ True if there are more results on the server """
        return (self.last_evaluated_key is not None and not self.limit.complete)

    def fetch(self):
        """ Fetch more results from Dynamo """
        self.limit.set_request_args(self.kwargs)
        data = self.connection.call(*self.args, **self.kwargs)
        self.limit.post_fetch(data)
        self.last_evaluated_key = data.get('LastEvaluatedKey')
        if self.last_evaluated_key is None:
            self.kwargs.pop('ExclusiveStartKey', None)
        else:
            self.kwargs['ExclusiveStartKey'] = self.last_evaluated_key
        self._update_capacity(data)
        if 'consumed_capacity' in data:
            self.consumed_capacity += data['consumed_capacity']
        for raw_item in data['Items']:
            item = self.connection.dynamizer.decode_keys(raw_item)
            if self.limit.accept(item):
                yield item


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


class TableResultSet(PagedIterator):

    """ Iterator that pages table names from ListTables """

    def __init__(self, connection, limit=None):
        super(TableResultSet, self).__init__()
        self.connection = connection
        self.limit = limit
        self.last_evaluated_table_name = None

    @property
    def can_fetch_more(self):
        if self.last_evaluated_table_name is None:
            return False
        return self.limit is None or self.limit > 0

    def fetch(self):
        kwargs = {}
        if self.limit is None:
            kwargs['Limit'] = 100
        else:
            kwargs['Limit'] = min(self.limit, 100)
        if self.last_evaluated_table_name is not None:
            kwargs['ExclusiveStartTableName'] = self.last_evaluated_table_name
        data = self.connection.call('list_tables', **kwargs)
        self.last_evaluated_table_name = data.get('LastEvaluatedTableName')
        tables = data['TableNames']
        if self.limit is not None:
            self.limit -= len(tables)
        return iter(tables)


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


class Limit(object):

    """
    Class that defines query/scan limit behavior

    Parameters
    ----------
    scan_limit : int, optional
        The maximum number of items for DynamoDB to scan. This will not
        necessarily be the number of items returned.
    item_limit : int, optional
        The maximum number of items to return. Fetches will continue until this
        number is reached or there are no results left. See also: ``strict``
    min_scan_limit : int, optional
        This only matters when ``item_limit`` is set and ``scan_limit`` is not.
        After doing multiple fetches, the ``item_limit`` may drop to a low
        value. The ``item_limit`` will be passed up as the query ``Limit``, but
        if your ``item_limit`` is down to 1 you may want to fetch more than 1
        item at a time. ``min_scan_limit`` determines the minimum ``Limit`` to
        send up when ``scan_limit`` is None. (default 20)
    strict : bool, optional
        This modifies the behavior of ``item_limit``. If True, the query will
        never return more items than ``item_limit``. If False, the query will
        fetch until it hits the ``item_limit``, and then return the rest of the
        page as well. (default False)
    filter : callable, optional
        Function that takes a single item dict and returns a boolean. If True,
        the item will be counted towards the ``item_limit`` and returned from
        the iterator. If False, it will be skipped.

    """

    def __init__(self, scan_limit=None, item_limit=None, min_scan_limit=20,
                 strict=False, filter=lambda x: True):
        self.scan_limit = scan_limit
        if item_limit is None:
            self.item_limit = scan_limit
        else:
            self.item_limit = item_limit
        self.min_scan_limit = min_scan_limit
        self.strict = strict
        self.filter = filter

    def copy(self):
        """ Return a copy of the limit """
        return Limit(self.scan_limit, self.item_limit, self.min_scan_limit,
                     self.strict, self.filter)

    def set_request_args(self, args):
        """ Set the Limit parameter into the request args """
        if self.scan_limit is not None:
            args['Limit'] = self.scan_limit
        elif self.item_limit is not None:
            args['Limit'] = max(self.item_limit, self.min_scan_limit)
        else:
            args.pop('Limit', None)

    @property
    def complete(self):
        """ Return True if the limit has been reached """
        if self.scan_limit is not None and self.scan_limit == 0:
            return True
        if self.item_limit is not None and self.item_limit == 0:
            return True
        return False

    def post_fetch(self, response):
        """ Called after a fetch. Updates the ScannedCount """
        if self.scan_limit is not None:
            self.scan_limit -= response['ScannedCount']

    def accept(self, item):
        """ Apply the filter and item_limit, and return True to accept """
        accept = self.filter(item)
        if accept and self.item_limit is not None:
            if self.item_limit > 0:
                self.item_limit -= 1
            elif self.strict:
                return False
        return accept
