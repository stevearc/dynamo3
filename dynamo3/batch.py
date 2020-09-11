""" Code for batch processing """
import logging

from .constants import MAX_WRITE_BATCH, NONE
from .types import is_null

LOG = logging.getLogger(__name__)


def _encode_write(dynamizer, data, action, key):
    """ Encode an item write command """
    # Strip null values out of data
    data = dict(((k, dynamizer.encode(v)) for k, v in data.items() if not is_null(v)))
    return {
        action: {
            key: data,
        }
    }


def encode_put(dynamizer, data):
    """ Encode an item put command """
    return _encode_write(dynamizer, data, "PutRequest", "Item")


def encode_delete(dynamizer, data):
    """ Encode an item delete command """
    return _encode_write(dynamizer, data, "DeleteRequest", "Key")


class BatchWriter(object):

    """ Context manager for writing a large number of items to a table """

    def __init__(
        self,
        connection,
        tablename,
        return_capacity=NONE,
        return_item_collection_metrics=NONE,
    ):
        self.connection = connection
        self.tablename = tablename
        self.return_capacity = return_capacity
        self.return_item_collection_metrics = return_item_collection_metrics
        self._to_put = []
        self._to_delete = []
        self._unprocessed = []
        self._attempt = 0
        self.consumed_capacity = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        # Don't try to flush remaining if we hit an exception
        if exc_type is not None:
            return
        # Flush anything that's left.
        if self._to_put or self._to_delete:
            self.flush()

        # Finally, handle anything that wasn't processed.
        if self._unprocessed:
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
        return len(self._to_put) + len(self._to_delete) == MAX_WRITE_BATCH

    def flush(self):
        """ Flush pending items to Dynamo """
        items = []

        for data in self._to_put:
            items.append(encode_put(self.connection.dynamizer, data))

        for data in self._to_delete:
            items.append(encode_delete(self.connection.dynamizer, data))
        self._write(items)
        self._to_put = []
        self._to_delete = []

    def _write(self, items):
        """ Perform a batch write and handle the response """
        response = self._batch_write_item(items)
        if "consumed_capacity" in response:
            # Comes back as a list from BatchWriteItem
            self.consumed_capacity = sum(
                response["consumed_capacity"], self.consumed_capacity
            )

        if response.get("UnprocessedItems"):
            unprocessed = response["UnprocessedItems"].get(self.tablename, [])

            # Some items have not been processed. Stow them for now &
            # re-attempt processing on ``__exit__``.
            LOG.info("%d items were unprocessed. Storing for later.", len(unprocessed))
            self._unprocessed.extend(unprocessed)
            # Getting UnprocessedItems indicates that we are exceeding our
            # throughput. So sleep for a bit.
            self._attempt += 1
            self.connection.exponential_sleep(self._attempt)
        else:
            # No UnprocessedItems means our request rate is fine, so we can
            # reset the attempt number.
            self._attempt = 0

        return response

    def resend_unprocessed(self):
        """ Resend all unprocessed items """
        LOG.info("Re-sending %d unprocessed items.", len(self._unprocessed))

        while self._unprocessed:
            to_resend = self._unprocessed[:MAX_WRITE_BATCH]
            self._unprocessed = self._unprocessed[MAX_WRITE_BATCH:]
            LOG.info("Sending %d items", len(to_resend))
            self._write(to_resend)
            LOG.info("%d unprocessed items left", len(self._unprocessed))

    def _batch_write_item(self, items):
        """ Make a BatchWriteItem call to Dynamo """
        kwargs = {
            "RequestItems": {
                self.tablename: items,
            },
            "ReturnConsumedCapacity": self.return_capacity,
            "ReturnItemCollectionMetrics": self.return_item_collection_metrics,
        }
        return self.connection.call("batch_write_item", **kwargs)
