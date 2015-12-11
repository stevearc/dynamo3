""" DynamoDB types and type logic """
import six
from decimal import Decimal, Context, Clamped, Overflow, Underflow

from .constants import (NUMBER, STRING, BINARY, NUMBER_SET, STRING_SET,
                        BINARY_SET, LIST, BOOL, MAP, NULL)


DECIMAL_CONTEXT = Context(Emin=-128, Emax=126, rounding=None, prec=38,
                          traps=[Clamped, Overflow, Underflow])


def float_to_decimal(f):
    """ Convert a float to a 38-precision Decimal """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    return DECIMAL_CONTEXT.divide(numerator, denominator)


TYPES = {
    'NUMBER': NUMBER,
    'STRING': STRING,
    'BINARY': BINARY,
    'NUMBER_SET': NUMBER_SET,
    'STRING_SET': STRING_SET,
    'BINARY_SET': BINARY_SET,
    'LIST': LIST,
    'BOOL': BOOL,
    'MAP': MAP,
    'NULL': NULL,
}
TYPES_REV = dict(((v, k) for k, v in six.iteritems(TYPES)))


def is_dynamo_value(value):
    """ Returns True if the value is a Dynamo-formatted value """
    if not isinstance(value, dict) or len(value) != 1:
        return False
    subkey = six.next(six.iterkeys(value))
    return subkey in TYPES_REV


def is_null(value):
    """ Check if a value is equivalent to null in Dynamo """
    return (value is None or
            (isinstance(value, (set, frozenset)) and len(value) == 0))


class Binary(object):

    """ Wrap a binary string """

    def __init__(self, value):
        if isinstance(value, six.text_type):
            value = value.encode('utf-8')
        if not isinstance(value, six.binary_type):
            raise TypeError('Value must be a string of binary data!')

        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        if isinstance(other, Binary):
            return self.value == other.value
        else:
            return self.value == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return 'Binary(%s)' % self.value


def encode_set(dynamizer, value):
    """ Encode a set for the DynamoDB format """
    inner_value = next(iter(value))
    inner_type = dynamizer.raw_encode(inner_value)[0]
    return inner_type + 'S', [dynamizer.raw_encode(v)[1] for v in value]


def encode_list(dynamizer, value):
    """ Encode a list for the DynamoDB format """
    encoded_list = []
    dict(map(dynamizer.raw_encode, value))
    for v in value:
        encoded_type, encoded_value = dynamizer.raw_encode(v)
        encoded_list.append({
            encoded_type: encoded_value,
        })
    return 'L', encoded_list


def encode_dict(dynamizer, value):
    """ Encode a dict for the DynamoDB format """
    encoded_dict = {}
    for k, v in six.iteritems(value):
        encoded_type, encoded_value = dynamizer.raw_encode(v)
        encoded_dict[k] = {
            encoded_type: encoded_value,
        }
    return 'M', encoded_dict


class Dynamizer(object):

    """ Handles the encoding/decoding of Dynamo values """

    def __init__(self):
        self.encoders = {}
        self.register_encoder(six.text_type, lambda _, v: (STRING, v))
        self.register_encoder(
            six.binary_type, lambda _, v: (
                STRING, v.decode('utf-8')))
        for t in six.integer_types:
            self.register_encoder(t, lambda _, v: (NUMBER, six.text_type(v)))
        self.register_encoder(
            float, lambda _, v: (
                NUMBER, six.text_type(
                    float_to_decimal(v))))
        self.register_encoder(
            Decimal, lambda _, v: (
                NUMBER, six.text_type(
                    DECIMAL_CONTEXT.create_decimal(v))))
        self.register_encoder(set, encode_set)
        self.register_encoder(frozenset, encode_set)
        self.register_encoder(Binary, lambda _, v: (BINARY, v.value))
        self.register_encoder(bool, lambda _, v: (BOOL, v))
        self.register_encoder(list, encode_list)
        self.register_encoder(dict, encode_dict)
        self.register_encoder(type(None), lambda _, v: (NULL, True))

    def register_encoder(self, type, encoder):
        """
        Set an encoder method for a data type

        Parameters
        ----------
        type : object
            The class of the data type to encode
        encoder : callable
            Accepts a (Dynamizer, value) and returns
            (dynamo_type, dynamo_value)

        """
        self.encoders[type] = encoder

    def raw_encode(self, value):
        """ Run the encoder on a value """
        if type(value) in self.encoders:
            encoder = self.encoders[type(value)]
            return encoder(self, value)
        raise ValueError("No encoder for value '%s' of type '%s'" %
                         (value, type(value)))

    def encode_keys(self, keys):
        """ Run the encoder on a dict of values """
        return dict(((k, self.encode(v)) for k, v in six.iteritems(keys) if not
                     is_null(v)))

    def maybe_encode_keys(self, keys):
        """ Same as encode_keys but a no-op if already in Dynamo format """
        ret = {}
        for k, v in six.iteritems(keys):
            if is_dynamo_value(v):
                return keys
            elif not is_null(v):
                ret[k] = self.encode(v)
        return ret

    def encode(self, value):
        """ Encode a value into the Dynamo dict format """
        return dict([self.raw_encode(value)])

    def decode_keys(self, keys):
        """ Run the decoder on a dict of values """
        return dict(((k, self.decode(v)) for k, v in six.iteritems(keys)))

    def decode(self, dynamo_value):
        """ Decode a dynamo value into a python value """
        type, value = next(six.iteritems(dynamo_value))
        if type == STRING:
            return value
        elif type == BINARY:
            return Binary(value)
        elif type == NUMBER:
            return Decimal(value)
        elif type == STRING_SET:
            return set(value)
        elif type == BINARY_SET:
            return set((Binary(v) for v in value))
        elif type == NUMBER_SET:
            return set((Decimal(v) for v in value))
        elif type == BOOL:
            return value
        elif type == LIST:
            return [self.decode(v) for v in value]
        elif type == MAP:
            decoded_dict = {}
            for k, v in six.iteritems(value):
                decoded_dict[k] = self.decode(v)
            return decoded_dict
        elif type == NULL:
            return None
        else:
            raise TypeError("Received unrecognized type %r from dynamo", type)
