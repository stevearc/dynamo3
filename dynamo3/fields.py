""" Objects for defining fields and indexes """
from abc import ABC, abstractmethod
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple, Union

from typing_extensions import Final, Literal

from .constants import (
    PAY_PER_REQUEST,
    STRING,
    BillingModeType,
    IndexStatusType,
    KeyType,
    StreamViewType,
    TableStatusType,
    TimeToLiveStatusType,
)
from .types import TYPES_REV


class TTL(NamedTuple):
    attribute_name: Optional[str]
    status: TimeToLiveStatusType

    @classmethod
    def default(cls) -> "TTL":
        return cls(None, "DISABLED")


class DynamoKey(object):

    """
    A single field inside a Dynamo table

    Parameters
    ----------
    name : str
        The name of the field
    data_type : {STRING, NUMBER, BINARY}
        The Dynamo data type of the field

    """

    def __init__(self, name: str, data_type: KeyType = STRING):
        self.name = name
        self.data_type = data_type

    def definition(self) -> Dict[str, str]:
        """ Returns the attribute definition """
        return {
            "AttributeName": self.name,
            "AttributeType": self.data_type,
        }

    def hash_schema(self):
        """ Get the schema definition with this field as a hash key """
        return self._schema("HASH")

    def range_schema(self):
        """ Get the schema definition with this field as a range key """
        return self._schema("RANGE")

    def _schema(
        self, key_type: Literal[Literal["HASH"], Literal["RANGE"]]
    ) -> Dict[str, str]:
        """ Construct the schema definition for this table """
        return {
            "AttributeName": self.name,
            "KeyType": key_type,
        }

    def __str__(self):
        return "DynamoKey(%s, %s)" % (self.name, TYPES_REV[self.data_type])

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (
            isinstance(other, DynamoKey)
            and self.name == other.name
            and self.data_type == other.data_type
        )

    def __ne__(self, other):
        return not self.__eq__(other)


ThroughputOrTuple = Union["Throughput", Tuple[int, int]]


class Throughput(object):

    """
    Representation of table or global index throughput

    Parameters
    ----------
    read : int, optional
        Read capacity throughput (default 5)
    write : int, optional
        Write capacity throughput (default 5)

    """

    def __init__(self, read: int = 5, write: int = 5):
        self.read = read
        self.write = write

    def __str__(self):
        return "Throughput({0}, {1})".format(self.read, self.write)

    def __repr__(self):
        return str(self)

    def __bool__(self):
        return bool(self.read and self.write)

    def schema(self) -> Dict[str, int]:
        """ Construct the schema definition for the throughput """
        return {
            "ReadCapacityUnits": self.read,
            "WriteCapacityUnits": self.write,
        }

    @classmethod
    def normalize(cls, instance: Optional[ThroughputOrTuple]) -> "Throughput":
        if instance is None:
            return cls(0, 0)
        if isinstance(instance, Throughput):
            return instance
        return cls(instance[0], instance[1])

    @classmethod
    def from_response(cls, response: Dict[str, int]) -> "Throughput":
        """ Create Throughput from returned Dynamo data """
        return cls(
            response["ReadCapacityUnits"],
            response["WriteCapacityUnits"],
        )

    def __hash__(self):
        return self.read + self.write

    def __eq__(self, other):
        return (
            isinstance(other, Throughput)
            and self.read == other.read
            and self.write == other.write
        )

    def __ne__(self, other):
        return not self.__eq__(other)


ProjectionType = Literal[Literal["ALL"], Literal["KEYS_ONLY"], Literal["INCLUDE"]]


class BaseIndex(object):

    """ Base class for indexes """

    ALL: Final[Literal["ALL"]] = "ALL"
    KEYS: Final[Literal["KEYS_ONLY"]] = "KEYS_ONLY"
    INCLUDE: Final[Literal["INCLUDE"]] = "INCLUDE"

    def __init__(
        self,
        projection_type: ProjectionType,
        name: str,
        range_key: Optional[DynamoKey],
        includes: Optional[List[str]],
    ):
        self.projection_type = projection_type
        self.name = name
        self.range_key = range_key
        self.include_fields = includes
        self.response: Dict[str, Any] = {}

    def __getitem__(self, key: str) -> Any:
        return self.response[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.response.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self.response

    def _schema(self, hash_key: DynamoKey) -> Dict[str, Any]:
        """
        Create the index schema

        Parameters
        ----------
        hash_key : :class:`~.DynamoKey`
            The hash key of the table

        """
        key_schema = [hash_key.hash_schema()]
        if self.range_key is not None:
            key_schema.append(self.range_key.range_schema())
        schema_data = {
            "IndexName": self.name,
            "KeySchema": key_schema,
        }
        projection: Any = {
            "ProjectionType": self.projection_type,
        }
        if self.include_fields is not None:
            projection["NonKeyAttributes"] = self.include_fields
        schema_data["Projection"] = projection
        return schema_data

    def __hash__(self):
        return hash(self.projection_type) + hash(self.name) + hash(self.range_key)

    def __eq__(self, other):
        return (
            type(other) == type(self)
            and self.projection_type == other.projection_type
            and self.name == other.name
            and self.range_key == other.range_key
            and self.include_fields == other.include_fields
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class LocalIndex(BaseIndex):

    """
    A local secondary index for a table

    You should generally use the factory methods :meth:`~.all`, :meth:`~.keys`,
    and :meth:`~.include` instead of the constructor.

    """

    range_key: DynamoKey

    def __init__(
        self,
        projection_type: ProjectionType,
        name: str,
        range_key: DynamoKey,
        includes: Optional[List[str]] = None,
    ):
        super(LocalIndex, self).__init__(projection_type, name, range_key, includes)

    @classmethod
    def all(cls, name: str, range_key: DynamoKey) -> "LocalIndex":
        """ Create an index that projects all attributes """
        return cls(cls.ALL, name, range_key)

    @classmethod
    def keys(cls, name: str, range_key: DynamoKey) -> "LocalIndex":
        """ Create an index that projects only key attributes """
        return cls(cls.KEYS, name, range_key)

    @classmethod
    def include(
        cls, name: str, range_key: DynamoKey, includes: List[str]
    ) -> "LocalIndex":
        """ Create an index that projects key attributes plus some others """
        return cls(cls.INCLUDE, name, range_key, includes)

    def schema(self, hash_key: DynamoKey) -> Dict[str, Any]:
        return super()._schema(hash_key)

    @classmethod
    def from_response(
        cls, response: Dict[str, Any], attrs: Dict[Any, Any]
    ) -> "LocalIndex":
        """ Create an index from returned Dynamo data """
        proj = response["Projection"]
        range_key = None
        for key_schema in response["KeySchema"]:
            if key_schema["KeyType"] == "RANGE":
                range_key = attrs[key_schema["AttributeName"]]
        if range_key is None:
            raise ValueError("No range key in local index definition")
        index = cls(
            proj["ProjectionType"],
            response["IndexName"],
            range_key,
            proj.get("NonKeyAttributes"),
        )
        index.response = response
        return index

    def __str__(self):
        if self.include_fields:
            return "LocalIndex(%s, %s, %s, [%s])" % (
                self.name,
                self.projection_type,
                self.range_key,
                ", ".join(self.include_fields),
            )
        else:
            return "LocalIndex(%s, %s, %s)" % (
                self.name,
                self.projection_type,
                self.range_key,
            )

    def __repr__(self):
        return "LocalIndex(%s)" % self.name


class GlobalIndex(BaseIndex):

    """
    A global secondary index for a table

    You should generally use the factory methods :meth:`~.all`, :meth:`~.keys`,
    and :meth:`~.include` instead of the constructor.

    """

    def __init__(
        self,
        projection_type: ProjectionType,
        name: str,
        hash_key: Optional[DynamoKey],
        range_key: Optional[DynamoKey] = None,
        includes: Optional[List[str]] = None,
        throughput: Optional[ThroughputOrTuple] = None,
        status: Optional[IndexStatusType] = None,
        backfilling: bool = False,
        item_count: int = 0,
        size: int = 0,
    ):
        super(GlobalIndex, self).__init__(projection_type, name, range_key, includes)
        self.hash_key = hash_key
        self.throughput = Throughput.normalize(throughput)
        self.status: Optional[IndexStatusType] = status
        self.backfilling = backfilling
        self.item_count = item_count
        self.size = size

    @classmethod
    def all(
        cls,
        name: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        throughput: Optional[ThroughputOrTuple] = None,
    ) -> "GlobalIndex":
        """ Create an index that projects all attributes """
        return cls(cls.ALL, name, hash_key, range_key, throughput=throughput)

    @classmethod
    def keys(
        cls,
        name: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        throughput: Optional[ThroughputOrTuple] = None,
    ) -> "GlobalIndex":
        """ Create an index that projects only key attributes """
        return cls(cls.KEYS, name, hash_key, range_key, throughput=throughput)

    @classmethod
    def include(
        cls,
        name: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        includes: Optional[List[str]] = None,
        throughput: Optional[ThroughputOrTuple] = None,
    ) -> "GlobalIndex":
        """ Create an index that projects key attributes plus some others """
        return cls(
            cls.INCLUDE, name, hash_key, range_key, includes, throughput=throughput
        )

    def schema(self) -> Dict[str, Any]:
        """ Construct the schema definition for this index """
        if self.hash_key is None:
            raise ValueError(
                "Cannot construct schema for index %r. Missing hash key" % self.name
            )
        schema_data = super(GlobalIndex, self)._schema(self.hash_key)
        if self.throughput:
            schema_data["ProvisionedThroughput"] = self.throughput.schema()
        return schema_data

    @classmethod
    def from_response(
        cls, response: Dict[str, Any], attrs: Dict[str, Any]
    ) -> "GlobalIndex":
        """ Create an index from returned Dynamo data """
        proj = response["Projection"]
        hash_key = None
        range_key = None
        for key_schema in response["KeySchema"]:
            key_attr = attrs.get(key_schema["AttributeName"])
            if key_schema["KeyType"] == "HASH":
                hash_key = key_attr
            if key_schema["KeyType"] == "RANGE":
                range_key = key_attr
        throughput = None
        if "ProvisionedThroughput" in response:
            throughput = Throughput.from_response(response["ProvisionedThroughput"])
        index = cls(
            proj["ProjectionType"],
            response["IndexName"],
            hash_key,
            range_key,
            proj.get("NonKeyAttributes"),
            throughput,
            response["IndexStatus"],
            response.get("Backfilling", False),
            response.get("ItemCount", 0),
            response.get("IndexSizeBytes", 0),
        )
        index.response = response
        return index

    def __str__(self):
        lines = ["GlobalIndex(%s, %s)" % (self.name, self.projection_type)]
        if self.hash_key:
            lines.append("Hash key: %s" % self.hash_key)
        if self.range_key:
            lines.append("Hash key: %s" % self.range_key)
        if self.include_fields:
            lines.append("Includes: %s" % ", ".join(self.include_fields))
        if self.throughput:
            lines.append("Throughput: %s" % self.throughput)

        return "\n  ".join(lines)

    def __repr__(self):
        return "GlobalIndex(%s)" % self.name

    def __hash__(self):  # pylint: disable=W0235
        return super().__hash__()

    def __eq__(self, other):
        return (
            super(GlobalIndex, self).__eq__(other)
            and self.hash_key == other.hash_key
            and self.throughput == other.throughput
        )


class RestoreSummary(NamedTuple):
    in_progress: bool
    time: int
    source_backup_arn: Optional[str]
    source_table_arn: Optional[str]

    @classmethod
    def from_response(cls, response: Dict[str, Any]) -> Optional["RestoreSummary"]:
        summary = response.get("RestoreSummary")
        if summary is None:
            return None
        return cls(
            response["RestoreInProgress"],
            response["RestoreDateTime"],
            response.get("SourceBackupArn"),
            response.get("SourceTableArn"),
        )

    @classmethod
    def default(cls):
        return cls(False, 0, None, None)


class SSEDescription(NamedTuple):
    status: Optional[
        Literal[
            Literal["ENABLING"],
            Literal["ENABLED"],
            Literal["DISABLING"],
            Literal["DISABLED"],
            Literal["UPDATING"],
        ]
    ]
    type: Optional[Literal[Literal["AES256"], Literal["KMS"]]]
    kms_arn: Optional[str]
    inaccessible_encryption_time: Optional[int]

    @classmethod
    def from_response(cls, response: Dict[str, Any]) -> Optional["SSEDescription"]:
        summary = response.get("SSEDescription")
        if summary is None:
            return None
        return cls(
            response.get("Status"),
            response.get("SSEType"),
            response.get("KMSMasterKeyArn"),
            response.get("InaccessibleEncryptionDateTime"),
        )

    @classmethod
    def default(cls):
        return cls(None, None, None, None)


class Table(object):

    """ Representation of a DynamoDB table """

    def __init__(
        self,
        name: str,
        # Hash key will not exist if table is DELETING
        hash_key: Optional[DynamoKey],
        range_key: Optional[DynamoKey] = None,
        indexes: Optional[List[LocalIndex]] = None,
        global_indexes: Optional[List[GlobalIndex]] = None,
        throughput: Optional[ThroughputOrTuple] = None,
        status: TableStatusType = "ACTIVE",
        billing_mode: Optional[BillingModeType] = None,
        arn: Optional[str] = None,
        stream_type: Optional[StreamViewType] = None,
        restore_summary: Optional[RestoreSummary] = None,
        sse_description: Optional[SSEDescription] = None,
        decreases_today: Optional[int] = None,
        item_count: int = 0,
        size: int = 0,
    ):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.indexes = indexes or []
        self.global_indexes = global_indexes or []
        self.throughput = Throughput.normalize(throughput)
        self.status = status
        self.billing_mode = billing_mode
        self.arn = arn
        self.stream_type = stream_type
        self.restore_summary = restore_summary or RestoreSummary.default()
        self.sse_description = sse_description or SSEDescription.default()
        self.decreases_today = decreases_today
        self.item_count = item_count
        self.size = size
        self.response: Dict[str, Any] = {}
        self.ttl: Optional[TTL] = None

    @property
    def attribute_definitions(self) -> Set[DynamoKey]:
        """ Getter for attribute_definitions """
        attrs = set()
        if self.hash_key is not None:
            attrs.add(self.hash_key)
        if self.range_key is not None:
            attrs.add(self.range_key)
        for index in self.indexes:
            attrs.add(index.range_key)
        for gindex in self.global_indexes:
            if gindex.hash_key is not None:
                attrs.add(gindex.hash_key)
            if gindex.range_key is not None:
                attrs.add(gindex.range_key)
        return attrs

    def __getitem__(self, key: str) -> Any:
        return self.response[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.response.get(key, default)

    def __contains__(self, key: str) -> bool:
        return key in self.response

    @classmethod
    def from_response(cls, response: Dict[str, Any]) -> "Table":
        """ Create a Table from returned Dynamo data """
        hash_key = None
        range_key = None
        # KeySchema may not be in the response if the TableStatus is DELETING.
        if "KeySchema" in response:
            attrs = dict(
                (
                    (
                        d["AttributeName"],
                        DynamoKey(d["AttributeName"], d["AttributeType"]),
                    )
                    for d in response["AttributeDefinitions"]
                )
            )
            for key_schema in response["KeySchema"]:
                key_attr = attrs[key_schema["AttributeName"]]
                if key_schema["KeyType"] == "HASH":
                    hash_key = key_attr
                if key_schema["KeyType"] == "RANGE":
                    range_key = key_attr

        indexes = []
        for idx in response.get("LocalSecondaryIndexes", []):
            indexes.append(LocalIndex.from_response(idx, attrs))
        global_indexes = []
        for idx in response.get("GlobalSecondaryIndexes", []):
            global_indexes.append(GlobalIndex.from_response(idx, attrs))
        throughput = None
        decreases_today = None
        if "ProvisionedThroughput" in response:
            throughput = Throughput.from_response(response["ProvisionedThroughput"])
            decreases_today = response["ProvisionedThroughput"][
                "NumberOfDecreasesToday"
            ]
        stream_type = None
        if (
            "StreamSpecification" in response
            and response["StreamSpecification"]["StreamEnabled"]
        ):
            stream_type = response["StreamSpecification"]["StreamViewType"]

        # TODO Replicas

        table = cls(
            name=response["TableName"],
            hash_key=hash_key,
            range_key=range_key,
            indexes=indexes,
            global_indexes=global_indexes,
            throughput=throughput,
            status=response["TableStatus"],
            billing_mode=response.get("BillingModeSummary", {}).get("BillingMode"),
            arn=response.get("TableArn"),
            stream_type=stream_type,
            restore_summary=RestoreSummary.from_response(response),
            sse_description=SSEDescription.from_response(response),
            decreases_today=decreases_today,
            item_count=response["ItemCount"],
            size=response["TableSizeBytes"],
        )
        table.response = response
        return table

    @property
    def is_on_demand(self):
        """ Getter for is_on_demand """
        return self.billing_mode == PAY_PER_REQUEST

    def __str__(self):
        lines = [
            "Table(%s)" % self.name,
        ]
        if self.hash_key is not None:
            lines.append("Hash key: %s" % self.hash_key)
        if self.range_key is not None:
            lines.append("Range key: %s" % self.range_key)
        if self.indexes:
            lines.append("Local indexes:")
            for index in self.indexes:
                lines.append("  %s" % index)
        if self.global_indexes:
            lines.append("Global indexes:")
            for gindex in self.global_indexes:
                lines.append("  %s" % gindex)
        return "\n  ".join(lines)

    def __repr__(self):
        return "Table(%s)" % self.name

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (
            isinstance(other, Table)
            and self.name == other.name
            and self.hash_key == other.hash_key
            and self.range_key == other.range_key
            and self.indexes == other.indexes
            and self.global_indexes == other.global_indexes
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class IndexUpdate(ABC):

    """
    An update to a GlobalSecondaryIndex to be passed to update_table

    You should generally use the factory methods :meth:`~update`,
    :meth:`~create`, and :meth:`~delete` instead of the constructor.

    """

    def __init__(
        self,
        action: Literal[Literal["Create"], Literal["Update"], Literal["Delete"]],
    ):
        self.action = action

    @staticmethod
    def update(index_name: str, throughput: ThroughputOrTuple) -> "IndexUpdateUpdate":
        """ Update the throughput on the index """
        return IndexUpdateUpdate(index_name, throughput)

    @staticmethod
    def create(index: GlobalIndex) -> "IndexUpdateCreate":
        """ Create a new index """
        return IndexUpdateCreate(index)

    @staticmethod
    def delete(index_name: str) -> "IndexUpdateDelete":
        """ Delete an index """
        return IndexUpdateDelete(index_name)

    @abstractmethod
    def get_attrs(self) -> List[DynamoKey]:
        """ Get all attrs necessary for the update (empty unless Create) """

    def serialize(self) -> Dict[str, Any]:
        """ Get the serialized Dynamo format for the update """
        return {self.action: self._get_schema()}

    @abstractmethod
    def _get_schema(self) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError()

    @abstractmethod
    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError()

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)


class IndexUpdateCreate(IndexUpdate):
    def __init__(
        self,
        index: GlobalIndex,
    ):
        super().__init__("Create")
        self.index = index

    def get_attrs(self) -> List[DynamoKey]:
        ret = []
        if self.index.hash_key is not None:
            ret.append(self.index.hash_key)
        if self.index.range_key is not None:
            ret.append(self.index.range_key)
        return ret

    def _get_schema(self):
        return self.index.schema()

    def __hash__(self):
        return hash(self.action) + hash(self.index)

    def __eq__(self, other):
        return (
            type(other) == type(self)
            and self.action == other.action
            and self.index == other.index
        )


class IndexUpdateUpdate(IndexUpdate):
    def __init__(
        self,
        index_name: str,
        throughput: ThroughputOrTuple,
    ):
        super().__init__("Update")
        self.index_name = index_name
        self.throughput = Throughput.normalize(throughput)

    def get_attrs(self) -> List[DynamoKey]:
        return []

    def _get_schema(self):
        return {
            "IndexName": self.index_name,
            "ProvisionedThroughput": self.throughput.schema(),
        }

    def __hash__(self):
        return hash(self.action) + hash(self.index_name) + hash(self.throughput)

    def __eq__(self, other):
        return (
            type(other) == type(self)
            and self.action == other.action
            and self.index_name == other.index_name
            and self.throughput == other.throughput
        )


class IndexUpdateDelete(IndexUpdate):
    def __init__(
        self,
        index_name: str,
    ):
        super().__init__("Delete")
        self.index_name = index_name

    def get_attrs(self) -> List[DynamoKey]:
        return []

    def _get_schema(self):
        return {
            "IndexName": self.index_name,
        }

    def __hash__(self):
        return hash(self.action) + hash(self.index_name)

    def __eq__(self, other):
        return (
            type(other) == type(self)
            and self.action == other.action
            and self.index_name == other.index_name
        )
