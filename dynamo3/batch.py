""" Code for batch processing """
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .constants import (
    MAX_WRITE_BATCH,
    ReturnCapacityType,
    ReturnItemCollectionMetricsType,
)
from .result import ConsumedCapacity
from .types import (
    Dynamizer,
    DynamoObject,
    ExpressionAttributeNamesType,
    ExpressionValuesType,
    ExpressionValueType,
    build_expression_values,
    is_null,
)

if TYPE_CHECKING:
    from .connection import DynamoDBConnection

LOG = logging.getLogger(__name__)


def _encode_write(
    dynamizer: Dynamizer, data: DynamoObject, action: str, key: str
) -> Dict:
    """ Encode an item write command """
    # Strip null values out of data
    data = dict(((k, dynamizer.encode(v)) for k, v in data.items() if not is_null(v)))
    return {
        action: {
            key: data,
        }
    }


def encode_put(dynamizer: Dynamizer, data: DynamoObject) -> Dict:
    """ Encode an item put command """
    return _encode_write(dynamizer, data, "PutRequest", "Item")


def encode_delete(dynamizer: Dynamizer, data: DynamoObject) -> Dict:
    """ Encode an item delete command """
    return _encode_write(dynamizer, data, "DeleteRequest", "Key")


class BatchWriter(object):

    """ Context manager for writing a large number of items to a table """

    def __init__(
        self,
        connection: "DynamoDBConnection",
        return_capacity: Optional[ReturnCapacityType] = None,
        return_item_collection_metrics: Optional[
            ReturnItemCollectionMetricsType
        ] = None,
    ):
        self.connection = connection
        self.return_capacity = return_capacity
        self.return_item_collection_metrics = return_item_collection_metrics
        self._to_put: List[Tuple[str, DynamoObject]] = []
        self._to_delete: List[Tuple[str, DynamoObject]] = []
        self._unprocessed: List[Tuple[str, DynamoObject]] = []
        self._attempt = 0
        self.consumed_capacity: Optional[Dict[str, ConsumedCapacity]] = None

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

    def put(self, tablename: str, data: DynamoObject) -> None:
        """
        Write an item (will overwrite existing data)

        Parameters
        ----------
        data : dict
            Item data

        """
        self._to_put.append((tablename, data))

        if self.should_flush:
            self.flush()

    def delete(self, tablename: str, kwargs: DynamoObject) -> None:
        """
        Delete an item

        Parameters
        ----------
        kwargs : dict
            The primary key of the item to delete

        """
        self._to_delete.append((tablename, kwargs))

        if self.should_flush:
            self.flush()

    @property
    def should_flush(self) -> bool:
        """ True if a flush is needed """
        return (
            len(self._to_put) + len(self._to_delete) + len(self._unprocessed)
            >= MAX_WRITE_BATCH
        )

    def flush(self) -> None:
        """ Flush pending items to Dynamo """
        table_map: Dict[str, List[Dict]] = {}
        count = 0

        for tablename, data in self._unprocessed:
            items = table_map.setdefault(tablename, [])
            items.append(data)
            count += 1
        self._unprocessed = []

        put_items, self._to_put = (
            self._to_put[0 : MAX_WRITE_BATCH - count],
            self._to_put[MAX_WRITE_BATCH - count :],
        )
        for tablename, data in put_items:
            count += 1
            items = table_map.setdefault(tablename, [])
            items.append(encode_put(self.connection.dynamizer, data))

        delete_items, self._to_delete = (
            self._to_delete[0 : MAX_WRITE_BATCH - count],
            self._to_delete[MAX_WRITE_BATCH - count :],
        )
        for tablename, data in delete_items:
            items = table_map.setdefault(tablename, [])
            items.append(encode_delete(self.connection.dynamizer, data))
        if table_map:
            self._write(table_map)
        # This will only happen if we're getting throttled hard. We shouldn't hit the
        # recursion limit because we'll be sleeping exponentially
        if self.should_flush:
            self.flush()

    def _write(self, table_map: Dict[str, List[Dict]]) -> None:
        """ Perform a batch write and handle the response """
        response = self._batch_write_item(table_map)
        if "consumed_capacity" in response:
            self.consumed_capacity = self.consumed_capacity or {}
            for cap in response["consumed_capacity"]:
                self.consumed_capacity[
                    cap.tablename
                ] = cap + self.consumed_capacity.get(cap.tablename)

        if "UnprocessedItems" in response:
            for tablename, unprocessed in response["UnprocessedItems"].items():
                # Some items have not been processed. Stow them for now &
                # re-attempt on the next try
                LOG.info(
                    "%d items were unprocessed. Storing for later.", len(unprocessed)
                )
                for item in unprocessed:
                    self._unprocessed.append((tablename, item))
                # Getting UnprocessedItems indicates that we are exceeding our
                # throughput. So sleep for a bit.
                self._attempt += 1
                self.connection.exponential_sleep(self._attempt)
        else:
            # No UnprocessedItems means our request rate is fine, so we can
            # reset the attempt number.
            self._attempt = 0

    def resend_unprocessed(self):
        """ Resend all unprocessed items """
        LOG.info("Re-sending %d unprocessed items.", len(self._unprocessed))

        while self._unprocessed:
            self.flush()
            LOG.info("%d unprocessed items left", len(self._unprocessed))

    def _batch_write_item(self, table_map: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """ Make a BatchWriteItem call to Dynamo """
        kwargs: Dict[str, Any] = {"RequestItems": table_map}
        if self.return_capacity is not None:
            kwargs["ReturnConsumedCapacity"] = self.return_capacity
        if self.return_item_collection_metrics is not None:
            kwargs["ReturnItemCollectionMetrics"] = self.return_item_collection_metrics
        return self.connection.call("batch_write_item", **kwargs)


class BatchWriterSingleTable(object):
    def __init__(self, tablename: str, writer: BatchWriter):
        self._tablename = tablename
        self._writer = writer

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._writer.__exit__(*args)

    def put(self, data: DynamoObject) -> None:
        self._writer.put(self._tablename, data)

    def delete(self, kwargs: DynamoObject) -> None:
        self._writer.delete(self._tablename, kwargs)

    def flush(self) -> None:
        self._writer.flush()

    @property
    def consumed_capacity(self) -> Optional[ConsumedCapacity]:
        """ Getter for consumed_capacity """
        cap_map = self._writer.consumed_capacity
        if cap_map is None:
            return None
        return cap_map[self._tablename]


class TransactionWriter(object):
    def __init__(
        self,
        connection: "DynamoDBConnection",
        token: Optional[str] = None,
        return_capacity: Optional[ReturnCapacityType] = None,
        return_item_collection_metrics: Optional[
            ReturnItemCollectionMetricsType
        ] = None,
    ):
        self._connection = connection
        self.token = token
        self._return_capacity = return_capacity
        self._return_item_collection_metrics = return_item_collection_metrics
        self._items: List[Dict] = []
        self.consumed_capacity: Optional[Dict[str, ConsumedCapacity]] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        # Don't try to flush remaining if we hit an exception
        if exc_type is not None:
            return
        self.execute()

    def _encode_action(
        self,
        __item_key: str,
        tablename: str,
        key: DynamoObject,
        condition: Optional[str],
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        **kwargs: ExpressionValueType,
    ) -> Dict[str, Any]:
        action = {
            "TableName": tablename,
            __item_key: self._connection.dynamizer.encode_keys(key),
        }
        if condition is not None:
            action["ConditionExpression"] = condition
        values = build_expression_values(
            self._connection.dynamizer, expr_values, kwargs
        )
        if values:
            action["ExpressionAttributeValues"] = values
        if alias:
            action["ExpressionAttributeNames"] = alias
        return action

    def check(
        self,
        tablename: str,
        key: DynamoObject,
        condition: str,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        **kwargs: ExpressionValueType,
    ) -> None:
        action = self._encode_action(
            "Key", tablename, key, condition, expr_values, alias, **kwargs
        )
        self._items.append({"ConditionCheck": action})

    def delete(
        self,
        tablename: str,
        key: DynamoObject,
        condition: Optional[str] = None,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        **kwargs: ExpressionValueType,
    ) -> None:
        action = self._encode_action(
            "Key", tablename, key, condition, expr_values, alias, **kwargs
        )
        self._items.append({"Delete": action})

    def put(
        self,
        tablename: str,
        item: DynamoObject,
        condition: Optional[str] = None,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        **kwargs: ExpressionValueType,
    ) -> None:
        action = self._encode_action(
            "Item", tablename, item, condition, expr_values, alias, **kwargs
        )
        self._items.append({"Put": action})

    def update(
        self,
        tablename: str,
        key: DynamoObject,
        expression: str,
        condition: Optional[str] = None,
        expr_values: Optional[ExpressionValuesType] = None,
        alias: Optional[ExpressionAttributeNamesType] = None,
        **kwargs: ExpressionValueType,
    ) -> None:
        action = self._encode_action(
            "Key", tablename, key, condition, expr_values, alias, **kwargs
        )
        action["UpdateExpression"] = expression
        self._items.append({"Update": action})

    def execute(self) -> None:
        kwargs: Dict[str, Any] = {"TransactItems": self._items}
        if self.token is not None:
            kwargs["ClientRequestToken"] = self.token
        if self._return_capacity is not None:
            kwargs["ReturnConsumedCapacity"] = self._return_capacity
        if self._return_item_collection_metrics is not None:
            kwargs["ReturnItemCollectionMetrics"] = self._return_item_collection_metrics
        response = self._connection.call("transact_write_items", **kwargs)

        if "consumed_capacity" in response:
            self.consumed_capacity = self.consumed_capacity or {}
            for cap in response["consumed_capacity"]:
                self.consumed_capacity[
                    cap.tablename
                ] = cap + self.consumed_capacity.get(cap.tablename)
