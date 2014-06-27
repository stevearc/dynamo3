""" Code for batch processing """
import logging
import six

from .util import is_null


LOG = logging.getLogger(__name__)
NO_ARG = object()

CONDITIONS = {
    'eq': 'EQ',
    'ne': 'NE',
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


class ItemUpdate(object):

    """
    An update operation for a single field on an item

    You should generally use the :meth:`~.put`, :meth:`~.add`, and
    :meth:`~.delete` methods instead of the constructor.

    Parameters
    ----------
    action : {ADD, DELETE, PUT}
        The action to take
    key : str
        The name of the field
    value : object, optional
        The new value for the field
    expected : object, optional
        DEPRECATED (use the comparison **kwargs instead).
        The expected current value for the field (None expects value to not
        exist)
    **kwargs : dict, optional
        Expected condition. See below.

    Examples
    --------
    You may pass in expected conditions using the operator as the key.

    .. code-block:: python

        # Put if value doesn't exist
        ItemUpdate.put('foo', 'bar', eq=None)
        # Add if value is less than 10
        ItemUpdate.add('foo', 14, lt=10)

    """

    ADD = 'ADD'
    DELETE = 'DELETE'
    PUT = 'PUT'

    def __init__(self, action, key, value=None, expected=NO_ARG, **kwargs):
        if is_null(value):
            if action == self.ADD:
                raise ValueError("Update must set a value for non-delete "
                                 "operations!")
            elif action == self.PUT:
                # If we are trying to PUT a null value, change to a delete
                action = self.DELETE
        self.action = action
        self.key = key
        self.value = value
        if expected is not NO_ARG:
            LOG.warn("Using deprecated argument 'expected' in ItemUpdate")
        self._expected = expected
        if len(kwargs) > 1:
            raise ValueError("Cannot have more than one condition on a single field")
        self._expect_kwargs = dict([(key + '__' + k, v) for k, v in
                                    six.iteritems(kwargs)])

    @classmethod
    def put(cls, *args, **kwargs):
        """ Shortcut for creating a PUT update """
        return cls(cls.PUT, *args, **kwargs)

    @classmethod
    def add(cls, *args, **kwargs):
        """ Shortcut for creating an ADD update """
        return cls(cls.ADD, *args, **kwargs)

    @classmethod
    def delete(cls, *args, **kwargs):
        """ Shortcut for creating a DELETE update """
        return cls(cls.DELETE, *args, **kwargs)

    def attrs(self, dynamizer):
        """ Get the attributes for the update """
        ret = {
            self.key: {
                'Action': self.action,
            }
        }
        if not is_null(self.value):
            ret[self.key]['Value'] = dynamizer.encode(self.value)
        return ret

    def expected(self, dynamizer):
        """ Get the expected values for the update """
        if self._expect_kwargs:
            return encode_query_kwargs(dynamizer, self._expect_kwargs)
        if self._expected is not NO_ARG:
            ret = {}
            if is_null(self._expected):
                ret['Exists'] = False
            else:
                ret['Value'] = dynamizer.encode(self._expected)
                ret['Exists'] = True
            return {self.key: ret}
        return {}

    def __hash__(self):
        return hash(self.action) + hash(self.key)

    def __eq__(self, other):
        return (isinstance(other, ItemUpdate) and
                self.action == other.action and
                self.key == other.key and
                self.value == other.value and
                self._expected == other._expected)

    def __ne__(self, other):
        return not self.__eq__(other)


def _encode_write(dynamizer, data, action, key):
    """ Encode an item write command """
    # Strip null values out of data
    data = dict(((k, dynamizer.encode(v)) for k, v in six.iteritems(data) if
                 not is_null(v)))
    return {
        action: {
            key: data,
        }
    }


def encode_put(dynamizer, data):
    """ Encode an item put command """
    return _encode_write(dynamizer, data, 'PutRequest', 'Item')


def encode_query_kwargs(dynamizer, kwargs):
    """ Encode query constraints in Dynamo format """
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
        elif not isinstance(v, (list, tuple, set, frozenset)):
            v = (v,)
        ret[name] = {
            'AttributeValueList': [dynamizer.encode(value) for value in v],
            'ComparisonOperator': CONDITIONS[condition_key],
        }
    return ret


def encode_delete(dynamizer, data):
    """ Encode an item delete command """
    return _encode_write(dynamizer, data, 'DeleteRequest', 'Key')


class BatchWriter(object):

    """ Context manager for writing a large number of items to a table """

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

    def put(self, data):
        """
        Write an item (will overwrite existing data)

        Parameters
        ----------
        data : dict
            Item data

        """
        self._to_put.append(data)

        if self.should_flush():
            self.flush()

    def delete(self, kwargs):
        """
        Delete an item

        Parameters
        ----------
        kwargs : dict
            The primary key of the item to delete

        """
        self._to_delete.append(kwargs)

        if self.should_flush():
            self.flush()

    def should_flush(self):
        """ True if a flush is needed """
        return len(self._to_put) + len(self._to_delete) == 25

    def flush(self):
        """ Flush pending items to Dynamo """
        items = []

        for data in self._to_put:
            items.append(encode_put(self.connection.dynamizer, data))

        for data in self._to_delete:
            items.append(encode_delete(self.connection.dynamizer, data))

        resp = self._batch_write_item(items)
        self._handle_unprocessed(resp)

        self._to_put = []
        self._to_delete = []

    def _handle_unprocessed(self, resp):
        """ Requeue unprocessed items """
        if resp.get('UnprocessedItems'):
            unprocessed = resp['UnprocessedItems'].get(self.tablename, [])

            # Some items have not been processed. Stow them for now &
            # re-attempt processing on ``__exit__``.
            LOG.info("%d items were unprocessed. Storing for later.",
                     len(unprocessed))
            self._unprocessed.extend(unprocessed)

    def resend_unprocessed(self):
        """ Resend all unprocessed items """
        LOG.info("Re-sending %d unprocessed items.", len(self._unprocessed))

        while self._unprocessed:
            to_resend = self._unprocessed[:25]
            self._unprocessed = self._unprocessed[25:]
            LOG.info("Sending %d items", len(to_resend))
            resp = self._batch_write_item(to_resend)
            self._handle_unprocessed(resp)
            LOG.info("%d unprocessed items left", len(self._unprocessed))

    def _batch_write_item(self, items):
        """ Make a BatchWriteItem call to Dynamo """
        data = {
            self.tablename: items,
        }
        return self.connection.call('BatchWriteItem', request_items=data)
