from .types import STRING


class DynamoKey(object):

    def __init__(self, name, data_type=STRING):
        self.name = name
        self.data_type = data_type

    def definition(self):
        """
        Returns the attribute definition structure DynamoDB expects.

        Example::

            >>> field.definition()
            {
                'AttributeName': 'username',
                'AttributeType': 'S',
            }

        """
        return {
            'AttributeName': self.name,
            'AttributeType': self.data_type,
        }

    def hash_schema(self):
        return self._schema('HASH')

    def range_schema(self):
        return self._schema('RANGE')

    def _schema(self, key_type):
        return {
            'AttributeName': self.name,
            'KeyType': key_type,
        }

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == getattr(other, 'name', None)

    def __ne__(self, other):
        return not self.__eq__(other)


class BaseIndexField(object):

    """
    An abstract class for defining schema indexes.

    Contains most of the core functionality for the index. Subclasses must
    define a ``projection_type`` to pass to DynamoDB.
    """
    projection_type = None

    def __init__(self, name, range_key):
        self.name = name
        self.range_key = range_key

    def schema(self, hash_key):
        """
        Returns the schema structure DynamoDB expects.

        Example::

            >>> index.schema()
            {
                'IndexName': 'LastNameIndex',
                'KeySchema': [
                    {
                        'AttributeName': 'username',
                        'KeyType': 'HASH',
                    },
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY',
                }
            }

        """
        return {
            'IndexName': self.name,
            'KeySchema': [
                hash_key.hash_schema(),
                self.range_key.range_schema(),
            ],
            'Projection': {
                'ProjectionType': self.projection_type,
            }
        }

    @classmethod
    def from_response(cls, response, attrs):
        return cls(response['IndexName'],
                   attrs[response['KeySchema'][1]['AttributeName']])

    def __hash__(self):
        return (hash(self.projection_type) + hash(self.name) +
                hash(self.range_key))

    def __eq__(self, other):
        return (isinstance(other, BaseIndexField) and
                self.projection_type == other.projection_type and
                self.name == other.name and
                self.range_key == other.range_key
                )

    def __ne__(self, other):
        return not self.__eq__(other)


class AllIndex(BaseIndexField):

    """
    An index signifying all fields should be in the index.

    Example::

        >>> AllIndex('MostRecentlyJoined', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ])

    """
    projection_type = 'ALL'


class KeysOnlyIndex(BaseIndexField):

    """
    An index signifying only key fields should be in the index.

    Example::

        >>> KeysOnlyIndex('MostRecentlyJoined', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ])

    """
    projection_type = 'KEYS_ONLY'


class IncludeIndex(BaseIndexField):

    """
    An index signifying only certain fields should be in the index.

    Example::

        >>> IncludeIndex('GenderIndex', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ], includes=['gender'])

    """
    projection_type = 'INCLUDE'

    def __init__(self, name, range_key, includes=None):
        super(IncludeIndex, self).__init__(name, range_key)
        self.includes_fields = includes or []

    def schema(self, hash_key):
        schema_data = super(IncludeIndex, self).schema(hash_key)
        schema_data['Projection']['NonKeyAttributes'] = self.includes_fields
        return schema_data

    @classmethod
    def from_response(cls, response, attrs):
        idx = super(IncludeIndex, cls).from_response(response, attrs)
        idx.includes_fields = response['Projection']['NonKeyAttributes']
        return idx

    def __eq__(self, other):
        return (
            super(IncludeIndex, self).__eq__(other) and
            self.includes_fields == other.includes_fields
        )


class GlobalBaseIndexField(BaseIndexField):

    """
    An abstract class for defining global indexes.

    Contains most of the core functionality for the index. Subclasses must
    define a ``projection_type`` to pass to DynamoDB.
    """

    def __init__(self, name, hash_key, range_key=None, throughput=None):
        super(GlobalBaseIndexField, self).__init__(name, range_key)
        self.hash_key = hash_key
        self.throughput = throughput or Throughput()

    def schema(self):
        """
        Returns the schema structure DynamoDB expects.

        Example::

            >>> index.schema()
            {
                'IndexName': 'LastNameIndex',
                'KeySchema': [
                    {
                        'AttributeName': 'username',
                        'KeyType': 'HASH',
                    },
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY',
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            }

        """
        key_schema = [self.hash_key.hash_schema()]
        if self.range_key is not None:
            key_schema.append(self.range_key.range_schema())
        return {
            'IndexName': self.name,
            'KeySchema': key_schema,
            'Projection': {
                'ProjectionType': self.projection_type,
            },
            'ProvisionedThroughput': self.throughput.schema(),
        }

    @classmethod
    def from_response(cls, response, attrs):
        hash_key = attrs[response['KeySchema'][0]['AttributeName']]
        range_key = None
        if len(response['KeySchema']) > 1:
            range_key = attrs[response['KeySchema'][1]['AttributeName']]
        throughput = Throughput.from_response(
            response['ProvisionedThroughput'])
        return cls(response['IndexName'], hash_key, range_key, throughput)

    def __eq__(self, other):
        return (
            super(GlobalBaseIndexField, self).__eq__(other) and
            self.throughput == other.throughput
        )


class GlobalAllIndex(GlobalBaseIndexField):

    """
    An index signifying all fields should be in the index.

    Example::

        >>> GlobalAllIndex('MostRecentlyJoined', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ],
        ... throughput={
        ...     'read': 2,
        ...     'write': 1,
        ... })

    """
    projection_type = 'ALL'


class GlobalKeysOnlyIndex(GlobalBaseIndexField):

    """
    An index signifying only key fields should be in the index.

    Example::

        >>> GlobalKeysOnlyIndex('MostRecentlyJoined', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ],
        ... throughput={
        ...     'read': 2,
        ...     'write': 1,
        ... })

    """
    projection_type = 'KEYS_ONLY'


class GlobalIncludeIndex(GlobalBaseIndexField):

    """
    An index signifying only certain fields should be in the index.

    Example::

        >>> GlobalIncludeIndex('GenderIndex', parts=[
        ...     HashKey('username'),
        ...     RangeKey('date_joined')
        ... ],
        ... includes=['gender'],
        ... throughput={
        ...     'read': 2,
        ...     'write': 1,
        ... })

    """
    projection_type = 'INCLUDE'

    def __init__(self, name, hash_key, range_key=None, throughput=None, includes=None):
        GlobalBaseIndexField.__init__(
            self, name, hash_key, range_key, throughput)
        self.includes_fields = includes

    def schema(self):
        schema_data = GlobalBaseIndexField.schema(self)
        schema_data['Projection']['NonKeyAttributes'] = self.includes_fields
        return schema_data

    @classmethod
    def from_response(cls, response, attrs):
        idx = super(GlobalIncludeIndex, cls).from_response(response, attrs)
        idx.includes_fields = response['Projection']['NonKeyAttributes']
        return idx

    def __eq__(self, other):
        return (
            super(GlobalIncludeIndex, self).__eq__(other) and
            self.includes_fields == other.includes_fields
        )


class Throughput(object):

    def __init__(self, read=5, write=5):
        self.read = read
        self.write = write

    def __repr__(self):
        return 'Throughput({0}, {1})'.format(self.read, self.write)

    def schema(self):
        return {
            'ReadCapacityUnits': self.read,
            'WriteCapacityUnits': self.write,
        }

    @classmethod
    def from_response(cls, response):
        return cls(
            response['ReadCapacityUnits'],
            response['WriteCapacityUnits'],
        )

    def __hash__(self):
        return self.read + self.write

    def __eq__(self, other):
        return (
            isinstance(other, Throughput) and
            self.read == other.read and
            self.write == other.write
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class Table(object):

    def __init__(self, name, hash_key, range_key=None, indexes=None,
                 global_indexes=None, throughput=None, status=None, size=0,
                 item_count=0):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.indexes = indexes or []
        self.global_indexes = global_indexes or []
        self.throughput = throughput or Throughput()
        self.status = status
        self.size = size
        self.item_count = item_count

    @classmethod
    def from_response(cls, response):
        attrs = dict(((d['AttributeName'],
                       DynamoKey(d['AttributeName'], d['AttributeType'])) for d
                      in response['AttributeDefinitions']))
        hash_key = attrs[response['KeySchema'][0]['AttributeName']]
        range_key = None
        if len(response['KeySchema']) > 1:
            range_key = attrs[response['KeySchema'][1]['AttributeName']]

        indexes = []
        index_types = {
            AllIndex.projection_type: AllIndex,
            KeysOnlyIndex.projection_type: KeysOnlyIndex,
            IncludeIndex.projection_type: IncludeIndex,
        }
        for idx in response.get('LocalSecondaryIndexes', []):
            idx_type = index_types[idx['Projection']['ProjectionType']]
            indexes.append(idx_type.from_response(idx, attrs))
        global_indexes = []
        index_types = {
            GlobalAllIndex.projection_type: GlobalAllIndex,
            GlobalKeysOnlyIndex.projection_type: GlobalKeysOnlyIndex,
            GlobalIncludeIndex.projection_type: GlobalIncludeIndex,
        }
        for idx in response.get('GlobalSecondaryIndexes', []):
            idx_type = index_types[idx['Projection']['ProjectionType']]
            global_indexes.append(idx_type.from_response(idx, attrs))

        return cls(
            name=response['TableName'],
            hash_key=hash_key,
            range_key=range_key,
            indexes=indexes,
            global_indexes=global_indexes,
            throughput=Throughput.from_response(
                response['ProvisionedThroughput']),
            status=response['TableStatus'],
            size=response['TableSizeBytes'],
            item_count=response['ItemCount'],
        )

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return (
            isinstance(other, Table) and
            self.name == other.name and
            self.hash_key == other.hash_key and
            self.range_key == other.range_key and
            self.indexes == other.indexes and
            self.global_indexes == other.global_indexes and
            self.throughput == other.throughput
        )

    def __ne__(self, other):
        return not self.__eq__(other)
