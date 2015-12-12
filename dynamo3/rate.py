""" Tools for rate limiting """
import six
import logging
import math
import time

LOG = logging.getLogger(__name__)


class DecayingCapacityStore(object):
    """
    Store for time series data that expires

    Parameters
    ----------
    window : int, optional
        Number of seconds in the window (default 1)

    """

    def __init__(self, window=1):
        self.window = window
        self.points = []

    def add(self, now, num):
        """ Add a timestamp and date to the data """
        if num == 0:
            return
        self.points.append((now, num))

    @property
    def value(self):
        """ Get the summation of all non-expired points """
        now = time.time()
        cutoff = now - self.window
        while self.points and self.points[0][0] < cutoff:
            self.points.pop(0)
        return sum([p[1] for p in self.points])


class RateLimit(object):
    """
    Class for rate limiting requests to DynamoDB

    Parameters
    ----------
    total_read : float, optional
        The overall maximum read units per second
    total_write : float, optional
        The overall maximum write units per second
    default_read : float, optional
        The default read unit cap for tables
    default_write : float, optional
        The default write unit cap for tables
    table_caps : dict, optional
        Mapping of table name to dicts with two optional keys ('read' and
        'write') that provide the maximum units for the table and local
        indexes. You can specity global indexes by adding a key to the dict or
        by providing another key in ``table_caps`` that is the table name and
        index name joined by a ``:``.
    """

    def __init__(self, total_read=None, total_write=None, default_read=None,
                 default_write=None, table_caps=None):
        self.total_cap = {
            'read': total_read,
            'write': total_write,
        }
        self.default_cap = {
            'read': default_read,
            'write': default_write,
        }
        self.table_caps = table_caps or {}
        self._old_default_return_capacity = False
        self._consumed = {}
        self._total_consumed = {
            'read': DecayingCapacityStore(),
            'write': DecayingCapacityStore(),
        }

    def install(self, connection):
        """ Install the throttle hooks onto a DynamoDBConnection """
        self._old_default_return_capacity = connection.default_return_capacity
        connection.default_return_capacity = True
        connection.subscribe('capacity', self.on_capacity)

    def uninstall(self, connection):
        """ Uninstall the throttle hooks from a DynamoDBConnection """
        connection.unsubscribe('capacity', self.on_capacity)
        connection.default_return_capacity = self._old_default_return_capacity

    def get_consumed(self, key):
        """ Getter for a consumed capacity storage dict """
        if key not in self._consumed:
            self._consumed[key] = {
                'read': DecayingCapacityStore(),
                'write': DecayingCapacityStore(),
            }
        return self._consumed[key]

    def on_capacity(self, connection, command, query_kwargs, response, capacity):
        """ Hook that runs in response to a 'returned capacity' event """
        now = time.time()
        # Check total against the total_cap
        self._wait(now, self.total_cap, self._total_consumed, capacity.total)

        # Increment table consumed capacity & check it
        if capacity.tablename in self.table_caps:
            table_cap = self.table_caps[capacity.tablename]
        else:
            table_cap = self.default_cap
        consumed_history = self.get_consumed(capacity.tablename)
        if capacity.table_capacity is not None:
            self._wait(now, table_cap, consumed_history, capacity.table_capacity)
        # The local index consumed capacity also counts against the table
        if capacity.local_index_capacity is not None:
            for consumed in six.itervalues(capacity.local_index_capacity):
                self._wait(now, table_cap, consumed_history, consumed)

        # Increment global indexes
        # check global indexes against the table+index cap or default
        gic = capacity.global_index_capacity
        if gic is not None:
            for index_name, consumed in six.iteritems(gic):
                full_name = capacity.tablename + ':' + index_name
                if index_name in table_cap:
                    index_cap = table_cap[index_name]
                elif full_name in self.table_caps:
                    index_cap = self.table_caps[full_name]
                else:
                    # If there's no specified capacity for the index,
                    # use the cap on the table
                    index_cap = table_cap
                consumed_history = self.get_consumed(full_name)
                self._wait(now, index_cap, consumed_history, consumed)

    def _wait(self, now, cap, consumed_history, consumed_capacity):
        """ Check the consumed capacity against the limit and sleep """
        for key in ['read', 'write']:
            if key in cap and cap[key] is not None:
                consumed_history[key].add(now, consumed_capacity[key])
                consumed = consumed_history[key].value
                if consumed > 0 and consumed >= cap[key]:
                    seconds = math.ceil(float(consumed) / cap[key])
                    LOG.debug("Rate limited throughput exceeded. Sleeping "
                              "for %d seconds.", seconds)
                    time.sleep(seconds)
