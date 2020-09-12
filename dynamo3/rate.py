""" Tools for rate limiting """
import logging
import math
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from typing_extensions import TypedDict

from .result import Capacity, ConsumedCapacity

if TYPE_CHECKING:
    from .connection import DynamoDBConnection

LOG = logging.getLogger(__name__)


class DecayingCapacityStore(object):
    """
    Store for time series data that expires

    Parameters
    ----------
    window : int, optional
        Number of seconds in the window (default 1)

    """

    def __init__(self, window: int = 1):
        self.window = window
        self.points: List[Tuple[float, int]] = []

    def add(self, now: float, num: int) -> None:
        """ Add a timestamp and date to the data """
        if num == 0:
            return
        self.points.append((now, num))

    @property
    def value(self) -> int:
        """ Get the summation of all non-expired points """
        now = time.time()
        cutoff = now - self.window
        while self.points and self.points[0][0] < cutoff:
            self.points.pop(0)
        return sum([p[1] for p in self.points])


RemainingCapacity = TypedDict(
    "RemainingCapacity", {"read": DecayingCapacityStore, "write": DecayingCapacityStore}
)


class RateLimit(object):
    """
    Class for rate limiting requests to DynamoDB

    Parameters
    ----------
    total_read : float, optional
        The overall maximum read units per second
    total_write : float, optional
        The overall maximum write units per second
    total : :class:`~dynamo3.result.Capacity`, optional
        A Capacity object. You can provide this instead of ``total_read`` and
        ``total_write``.
    default_read : float, optional
        The default read unit cap for tables
    default_write : float, optional
        The default write unit cap for tables
    default : :class:`~dynamo3.result.Capacity`, optional
        A Capacity object. You can provide this instead of ``default_read`` and
        ``default_write``.
    table_caps : dict, optional
        Mapping of table name to dicts with two optional keys ('read' and
        'write') that provide the maximum units for the table and local
        indexes. You can specify global indexes by adding a key to the dict or
        by providing another key in ``table_caps`` that is the table name and
        index name joined by a ``:``.
    callback : callable, optional
        A function that will be called if the rate limit is exceeded. It will
        be called with (connection, command, query, response,
        consumed_capacity, seconds). If this function returns True, RateLimit
        will skip the sleep.
    """

    def __init__(
        self,
        total_read: int = 0,
        total_write: int = 0,
        total: Optional[Capacity] = None,
        default_read: int = 0,
        default_write: int = 0,
        default: Optional[Capacity] = None,
        table_caps: Optional[Dict[str, Any]] = None,
        callback: Optional[Callable] = None,
    ):
        if total is not None:
            self.total_cap = total
        else:
            self.total_cap = Capacity(total_read, total_write)
        if default is not None:
            self.default_cap = default
        else:
            self.default_cap = Capacity(default_read, default_write)
        self.table_caps = table_caps or {}
        self._old_default_return_capacity = False
        self._consumed: Dict[str, RemainingCapacity] = {}
        self._total_consumed = {
            "read": DecayingCapacityStore(),
            "write": DecayingCapacityStore(),
        }
        self.callback = callback

    def get_consumed(self, key: str) -> RemainingCapacity:
        """ Getter for a consumed capacity storage dict """
        if key not in self._consumed:
            self._consumed[key] = {
                "read": DecayingCapacityStore(),
                "write": DecayingCapacityStore(),
            }
        return self._consumed[key]

    def on_capacity(
        self,
        connection: "DynamoDBConnection",
        command: str,
        query_kwargs: Dict[str, Any],
        response: Dict[str, Any],
        capacity: ConsumedCapacity,
    ) -> None:
        """ Hook that runs in response to a 'returned capacity' event """
        now = time.time()
        args = (connection, command, query_kwargs, response, capacity)
        # Check total against the total_cap
        self._wait(args, now, self.total_cap, self._total_consumed, capacity.total)

        # Increment table consumed capacity & check it
        if capacity.tablename in self.table_caps:
            table_cap = self.table_caps[capacity.tablename]
        else:
            table_cap = self.default_cap
        consumed_history = self.get_consumed(capacity.tablename)
        if capacity.table_capacity is not None:
            self._wait(args, now, table_cap, consumed_history, capacity.table_capacity)
        # The local index consumed capacity also counts against the table
        if capacity.local_index_capacity is not None:
            for consumed in capacity.local_index_capacity.values():
                self._wait(args, now, table_cap, consumed_history, consumed)

        # Increment global indexes
        # check global indexes against the table+index cap or default
        gic = capacity.global_index_capacity
        if gic is not None:
            for index_name, consumed in gic.items():
                full_name = capacity.tablename + ":" + index_name
                if index_name in table_cap:
                    index_cap = table_cap[index_name]
                elif full_name in self.table_caps:
                    index_cap = self.table_caps[full_name]
                else:
                    # If there's no specified capacity for the index,
                    # use the cap on the table
                    index_cap = table_cap
                consumed_history = self.get_consumed(full_name)
                self._wait(args, now, index_cap, consumed_history, consumed)

    def _wait(self, args, now, cap, consumed_history, consumed_capacity):
        """ Check the consumed capacity against the limit and sleep """
        for key in ["read", "write"]:
            if key in cap and cap[key] > 0:
                consumed_history[key].add(now, consumed_capacity[key])
                consumed = consumed_history[key].value
                if consumed > 0 and consumed >= cap[key]:
                    seconds = math.ceil(float(consumed) / cap[key])
                    LOG.debug(
                        "Rate limited throughput exceeded. Sleeping " "for %d seconds.",
                        seconds,
                    )
                    if callable(self.callback):
                        callback_args = args + (seconds,)
                        if self.callback(*callback_args):
                            continue
                    time.sleep(seconds)
