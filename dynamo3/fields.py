""" Objects for defining fields and indexes """
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from typing_extensions import Final, Literal

from .constants import STRING, KeyType, TableStatusType
from .util import snake_to_camel


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

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == getattr(other, "name", None)

    def __ne__(self, other):
        return not self.__eq__(other)


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

    def __repr__(self):
        return "Throughput({0}, {1})".format(self.read, self.write)

    def schema(self) -> Dict[str, int]:
        """ Construct the schema definition for the throughput """
        return {
            "ReadCapacityUnits": self.read,
            "WriteCapacityUnits": self.write,
        }

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

    def __getattr__(self, name: str) -> Any:
        camel_name = snake_to_camel(name)
        if camel_name in self.response:
            return self.response[camel_name]
        return super(BaseIndex, self).__getattribute__(name)

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
        index = cls(
            proj["ProjectionType"],
            response["IndexName"],
            attrs[response["KeySchema"][1]["AttributeName"]],
            proj.get("NonKeyAttributes"),
        )
        index.response = response
        return index


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
        throughput: Optional[Throughput] = None,
    ):
        super(GlobalIndex, self).__init__(projection_type, name, range_key, includes)
        self.hash_key = hash_key
        self.throughput = throughput or Throughput()

    @classmethod
    def all(
        cls,
        name: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        throughput: Optional[Throughput] = None,
    ) -> "GlobalIndex":
        """ Create an index that projects all attributes """
        return cls(cls.ALL, name, hash_key, range_key, throughput=throughput)

    @classmethod
    def keys(
        cls,
        name: str,
        hash_key: DynamoKey,
        range_key: Optional[DynamoKey] = None,
        throughput: Optional[Throughput] = None,
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
        throughput: Optional[Throughput] = None,
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
        schema_data["ProvisionedThroughput"] = self.throughput.schema()
        return schema_data

    @classmethod
    def from_response(
        cls, response: Dict[str, Any], attrs: Dict[str, Any]
    ) -> "GlobalIndex":
        """ Create an index from returned Dynamo data """
        proj = response["Projection"]
        hash_key = attrs.get(response["KeySchema"][0]["AttributeName"])
        range_key = None
        if len(response["KeySchema"]) > 1:
            range_key = attrs[response["KeySchema"][1]["AttributeName"]]
        throughput = Throughput.from_response(response["ProvisionedThroughput"])
        index = cls(
            proj["ProjectionType"],
            response["IndexName"],
            hash_key,
            range_key,
            proj.get("NonKeyAttributes"),
            throughput,
        )
        index.response = response
        return index

    def __hash__(self):  # pylint: disable=W0235
        return super().__hash__()

    def __eq__(self, other):
        return (
            super(GlobalIndex, self).__eq__(other)
            and self.hash_key == other.hash_key
            and self.throughput == other.throughput
        )


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
        throughput: Optional[Throughput] = None,
        status=None,
        size: int = 0,
    ):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.indexes = indexes or []
        self.global_indexes = global_indexes or []
        self.throughput = throughput or Throughput()
        self.status: TableStatusType = status
        self.size = size
        self.response: Dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        camel_name = snake_to_camel(name)
        if camel_name in self.response:
            return self.response[camel_name]
        return super(Table, self).__getattribute__(name)

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
            hash_key = attrs[response["KeySchema"][0]["AttributeName"]]
            if len(response["KeySchema"]) > 1:
                range_key = attrs[response["KeySchema"][1]["AttributeName"]]

        indexes = []
        for idx in response.get("LocalSecondaryIndexes", []):
            indexes.append(LocalIndex.from_response(idx, attrs))
        global_indexes = []
        for idx in response.get("GlobalSecondaryIndexes", []):
            global_indexes.append(GlobalIndex.from_response(idx, attrs))

        table = cls(
            name=response["TableName"],
            hash_key=hash_key,
            range_key=range_key,
            indexes=indexes,
            global_indexes=global_indexes,
            throughput=Throughput.from_response(response["ProvisionedThroughput"]),
            status=response["TableStatus"],
            size=response["TableSizeBytes"],
        )
        table.response = response
        return table

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
            and self.throughput == other.throughput
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
    def update(index_name: str, throughput: Throughput) -> "IndexUpdateUpdate":
        """ Update the throughput on the index """
        return IndexUpdateUpdate(index_name, throughput)

    @staticmethod
    def create(index: BaseIndex) -> "IndexUpdateCreate":
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
    def __eq__(self, other) -> bool:
        raise NotImplementedError()

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)


class IndexUpdateCreate(IndexUpdate):
    def __init__(
        self,
        index: BaseIndex,
    ):
        super().__init__("Create")
        self.index = index

    def get_attrs(self):
        ret = [self.index.hash_key]
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
        throughput: Throughput,
    ):
        super().__init__("Update")
        self.index_name = index_name
        self.throughput = throughput

    def get_attrs(self):
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

    def get_attrs(self):
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
