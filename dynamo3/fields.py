""" Objects for defining fields and indexes """
from .types import STRING
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

    def __init__(self, name, data_type=STRING):
        self.name = name
        self.data_type = data_type

    def definition(self):
        """ Returns the attribute definition """
        return {
            'AttributeName': self.name,
            'AttributeType': self.data_type,
        }

    def hash_schema(self):
        """ Get the schema definition with this field as a hash key """
        return self._schema('HASH')

    def range_schema(self):
        """ Get the schema definition with this field as a range key """
        return self._schema('RANGE')

    def _schema(self, key_type):
        """ Construct the schema definition for this table """
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


class BaseIndex(object):

    """ Base class for indexes """
    ALL = 'ALL'
    KEYS = 'KEYS_ONLY'
    INCLUDE = 'INCLUDE'

    def __init__(self, projection_type, name, range_key, includes):
        self.projection_type = projection_type
        self.name = name
        self.range_key = range_key
        self.include_fields = includes
        self.response = {}

    def __getattr__(self, name):
        camel_name = snake_to_camel(name)
        if camel_name in self.response:
            return self.response[camel_name]
        return super(BaseIndex, self).__getattribute__(name)

    def schema(self, hash_key):
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
            'IndexName': self.name,
            'KeySchema': key_schema,
            'Projection': {
                'ProjectionType': self.projection_type,
            }
        }
        if self.include_fields is not None:
            schema_data['Projection']['NonKeyAttributes'] = self.include_fields
        return schema_data

    def __hash__(self):
        return (hash(self.projection_type) + hash(self.name) +
                hash(self.range_key))

    def __eq__(self, other):
        return (type(other) == type(self) and
                self.projection_type == other.projection_type and
                self.name == other.name and
                self.range_key == other.range_key and
                self.include_fields == other.include_fields
                )

    def __ne__(self, other):
        return not self.__eq__(other)


class LocalIndex(BaseIndex):

    """
    A local secondary index for a table

    You should generally use the factory methods :meth:`~.all`, :meth:`~.keys`,
    and :meth:`~.include` instead of the constructor.

    """

    def __init__(self, projection_type, name, range_key, includes=None):
        super(LocalIndex, self).__init__(projection_type, name, range_key,
                                         includes)

    @classmethod
    def all(cls, name, range_key):
        """ Create an index that projects all attributes """
        return cls(cls.ALL, name, range_key)

    @classmethod
    def keys(cls, name, range_key):
        """ Create an index that projects only key attributes """
        return cls(cls.KEYS, name, range_key)

    @classmethod
    def include(cls, name, range_key, includes):
        """ Create an index that projects key attributes plus some others """
        return cls(cls.INCLUDE, name, range_key, includes)

    @classmethod
    def from_response(cls, response, attrs):
        """ Create an index from returned Dynamo data """
        proj = response['Projection']
        index = cls(proj['ProjectionType'], response['IndexName'],
                    attrs[response['KeySchema'][1]['AttributeName']],
                    proj.get('NonKeyAttributes'))
        index.response = response
        return index


class GlobalIndex(BaseIndex):

    """
    A global secondary index for a table

    You should generally use the factory methods :meth:`~.all`, :meth:`~.keys`,
    and :meth:`~.include` instead of the constructor.

    """

    def __init__(self, projection_type, name, hash_key, range_key=None,
                 includes=None, throughput=None):
        super(GlobalIndex, self).__init__(projection_type, name, range_key,
                                          includes)
        self.hash_key = hash_key
        self.throughput = throughput or Throughput()

    @classmethod
    def all(cls, name, hash_key, range_key=None, throughput=None):
        """ Create an index that projects all attributes """
        return cls(cls.ALL, name, hash_key, range_key, throughput=throughput)

    @classmethod
    def keys(cls, name, hash_key, range_key=None, throughput=None):
        """ Create an index that projects only key attributes """
        return cls(cls.KEYS, name, hash_key, range_key,
                   throughput=throughput)

    @classmethod
    def include(cls, name, hash_key, range_key=None, includes=None,
                throughput=None):
        """ Create an index that projects key attributes plus some others """
        return cls(cls.INCLUDE, name, hash_key, range_key, includes,
                   throughput=throughput)

    def schema(self):
        """ Construct the schema definition for this index """
        schema_data = super(GlobalIndex, self).schema(self.hash_key)
        schema_data['ProvisionedThroughput'] = self.throughput.schema()
        return schema_data

    @classmethod
    def from_response(cls, response, attrs):
        """ Create an index from returned Dynamo data """
        proj = response['Projection']
        hash_key = attrs.get(response['KeySchema'][0]['AttributeName'])
        range_key = None
        if len(response['KeySchema']) > 1:
            range_key = attrs[response['KeySchema'][1]['AttributeName']]
        throughput = Throughput.from_response(
            response['ProvisionedThroughput'])
        index = cls(proj['ProjectionType'], response['IndexName'], hash_key,
                    range_key, proj.get('NonKeyAttributes'), throughput)
        index.response = response
        return index

    def __hash__(self):
        return super(GlobalIndex, self).__hash__()

    def __eq__(self, other):
        return (
            super(GlobalIndex, self).__eq__(other) and
            self.hash_key == other.hash_key and
            self.throughput == other.throughput
        )


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

    def __init__(self, read=5, write=5):
        self.read = read
        self.write = write

    def __repr__(self):
        return 'Throughput({0}, {1})'.format(self.read, self.write)

    def schema(self):
        """ Construct the schema definition for the throughput """
        return {
            'ReadCapacityUnits': self.read,
            'WriteCapacityUnits': self.write,
        }

    @classmethod
    def from_response(cls, response):
        """ Create Throughput from returned Dynamo data """
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

    """ Representation of a DynamoDB table """

    def __init__(self, name, hash_key, range_key=None, indexes=None,
                 global_indexes=None, throughput=None, status=None, size=0):
        self.name = name
        self.hash_key = hash_key
        self.range_key = range_key
        self.indexes = indexes or []
        self.global_indexes = global_indexes or []
        self.throughput = throughput or Throughput()
        self.status = status
        self.size = size
        self.response = {}

    def __getattr__(self, name):
        camel_name = snake_to_camel(name)
        if camel_name in self.response:
            return self.response[camel_name]
        return super(Table, self).__getattribute__(name)

    @classmethod
    def from_response(cls, response):
        """ Create a Table from returned Dynamo data """
        hash_key = None
        range_key = None
        # KeySchema may not be in the response if the TableStatus is DELETING.
        if 'KeySchema' in response:
            attrs = dict(((d['AttributeName'],
                           DynamoKey(d['AttributeName'], d['AttributeType']))
                          for d in response['AttributeDefinitions']))
            hash_key = attrs[response['KeySchema'][0]['AttributeName']]
            if len(response['KeySchema']) > 1:
                range_key = attrs[response['KeySchema'][1]['AttributeName']]

        indexes = []
        for idx in response.get('LocalSecondaryIndexes', []):
            indexes.append(LocalIndex.from_response(idx, attrs))
        global_indexes = []
        for idx in response.get('GlobalSecondaryIndexes', []):
            global_indexes.append(GlobalIndex.from_response(idx, attrs))

        table = cls(
            name=response['TableName'],
            hash_key=hash_key,
            range_key=range_key,
            indexes=indexes,
            global_indexes=global_indexes,
            throughput=Throughput.from_response(
                response['ProvisionedThroughput']),
            status=response['TableStatus'],
            size=response['TableSizeBytes'],
        )
        table.response = response
        return table

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


class IndexUpdate(object):

    """
    An update to a GlobalSecondaryIndex to be passed to update_table

    You should generally use the factory methods :meth:`~update`,
    :meth:`~create`, and :meth:`~delete` instead of the constructor.

    """

    def __init__(self, action, index_name, **kwargs):
        self.action = action
        self.index_name = index_name
        self.extra = kwargs

    @classmethod
    def update(cls, index_name, throughput):
        """ Update the throughput on the index """
        return cls('Update', index_name, throughput=throughput)

    @classmethod
    def create(cls, index):
        """ Create a new index """
        return cls('Create', None, index=index)

    @classmethod
    def delete(cls, index_name):
        """ Delete an index """
        return cls('Delete', index_name)

    def get_attrs(self):
        """ Get all attrs necessary for the update (empty unless Create) """
        if self.action != 'Create':
            return []
        index = self.extra['index']
        ret = [index.hash_key]
        if index.range_key is not None:
            ret.append(index.range_key)
        return ret

    def serialize(self):
        """ Get the serialized Dynamo format for the update """
        if self.action == 'Create':
            payload = self.extra['index'].schema()
        else:
            payload = {
                'IndexName': self.index_name,
            }
            if self.action == 'Update':
                payload['ProvisionedThroughput'] = \
                    self.extra['throughput'].schema()
        return {
            self.action: payload
        }

    def __hash__(self):
        return hash(self.action) + hash(self.index_name)

    def __eq__(self, other):
        return (type(other) == type(self) and
                self.action == other.action and
                self.index_name == other.index_name and
                self.extra == other.extra)

    def __ne__(self, other):
        return not self.__eq__(other)
