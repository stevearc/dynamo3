""" Tests for rate limiting """
import time

from contextlib import contextmanager
from mock import patch

from . import BaseSystemTest
from dynamo3 import DynamoKey, GlobalIndex, RateLimit
from dynamo3.rate import DecayingCapacityStore
from dynamo3.result import ConsumedCapacity, Capacity


class TestRateLimit(BaseSystemTest):

    """ Tests for rate limiting """

    def setUp(self):
        super(TestRateLimit, self).setUp()
        hash_key = DynamoKey('id')
        index_key = DynamoKey('bar')
        index = GlobalIndex.all('bar', index_key)
        self.dynamo.create_table('foobar', hash_key, global_indexes=[index])

    @contextmanager
    def inject_capacity(self, capacity, limiter):
        """ Install limiter and inject consumed_capacity into response """
        def injector(connection, command, kwargs, data):
            """ Hook that injects consumed_capacity """
            data['consumed_capacity'] = capacity
            # We have to manually call the capacity hooks because they're only
            # run if the response has a ConsumedCapacity key (which we're
            # bypassing)
            for hook in connection._hooks['capacity']:
                hook(connection, command, kwargs, data, capacity)
        limiter.install(self.dynamo)
        self.dynamo.subscribe('postcall', injector)
        try:
            with patch.object(time, 'sleep') as sleep:
                yield sleep
        finally:
            limiter.uninstall(self.dynamo)
            self.dynamo.unsubscribe('postcall', injector)

    def test_no_throttle(self):
        """ Don't sleep if consumed capacity is within limits """
        limiter = RateLimit(3, 3)
        cap = ConsumedCapacity('foobar', Capacity(0, 2))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_not_called()

    def test_throttle_total(self):
        """ Sleep if consumed capacity exceeds total """
        limiter = RateLimit(3, 3)
        cap = ConsumedCapacity('foobar', Capacity(3, 0))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(1)

    def test_throttle_multiply(self):
        """ Seconds to sleep is increades to match limit delta """
        limiter = RateLimit(3, 3)
        cap = ConsumedCapacity('foobar', Capacity(8, 0))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(3)

    def test_throttle_multiple(self):
        """ Sleep if the limit is exceeded by multiple calls """
        limiter = RateLimit(4, 4)
        cap = ConsumedCapacity('foobar', Capacity(3, 0))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_throttle_table(self):
        """ Sleep if table limit is exceeded """
        limiter = RateLimit(3, 3, table_caps={
            'foobar': Capacity(0, 4),
        })
        cap = ConsumedCapacity('foobar', Capacity(8, 0), Capacity(0, 8))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_throttle_table_default(self):
        """ If no table limit provided, use the default """
        limiter = RateLimit(default_read=4, default_write=4)
        cap = ConsumedCapacity('foobar', Capacity(8, 0), Capacity(8, 0))
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_local_index(self):
        """ Local index capacities count towards the table limit """
        limiter = RateLimit(table_caps={
            'foobar': Capacity(4, 0),
        })
        cap = ConsumedCapacity('foobar', Capacity(8, 0), local_index_capacity={
            'local': Capacity(4, 0),
        })
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(1)

    def test_global_index(self):
        """ Sleep when global index limit is exceeded """
        limiter = RateLimit(table_caps={
            'foobar': {
                'baz': Capacity(4, 0),
            }
        })
        cap = ConsumedCapacity('foobar', Capacity(8, 0), global_index_capacity={
            'baz': Capacity(8, 0),
        })
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_global_index_by_name(self):
        """ Global index limit can be specified as tablename:index_name """
        limiter = RateLimit(table_caps={
            'foobar:baz': Capacity(4, 0),
        })
        cap = ConsumedCapacity('foobar', Capacity(8, 0), global_index_capacity={
            'baz': Capacity(8, 0),
        })
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_global_default_table(self):
        """ Global index limit defaults to table limit if not present """
        limiter = RateLimit(table_caps={
            'foobar': Capacity(4, 0),
        })
        cap = ConsumedCapacity('foobar', Capacity(8, 0), global_index_capacity={
            'baz': Capacity(8, 0),
        })
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_global_default(self):
        """ Global index limit will fall back to table default limit """
        limiter = RateLimit(default_read=4, default_write=4)
        cap = ConsumedCapacity('foobar', Capacity(8, 0), global_index_capacity={
            'baz': Capacity(8, 0),
        })
        with self.inject_capacity(cap, limiter) as sleep:
            list(self.dynamo.query2('foobar', 'id = :id', id='a'))
        sleep.assert_called_with(2)

    def test_store_decays(self):
        """ DecayingCapacityStore should drop points after time """
        store = DecayingCapacityStore()
        store.add(time.time() - 2, 4)
        self.assertEqual(store.value, 0)
