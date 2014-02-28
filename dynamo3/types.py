""" DynamoDB types and type logic """
import base64
import six
from decimal import Decimal, Context, Clamped, Overflow, Underflow


DECIMAL_CONTEXT = Context(
    Emin=-128, Emax=126, rounding=None, prec=38,
    traps=[Clamped, Overflow, Underflow])


def float_to_decimal(f):  # pragma: no cover
    """ Convert a float to a 38-precision Decimal """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    return DECIMAL_CONTEXT.divide(numerator, denominator)


NUMBER = 'N'
STRING = 'S'
BINARY = 'B'
NUMBER_SET = 'NS'
STRING_SET = 'SS'
BINARY_SET = 'BS'

TYPES = {
    'NUMBER': NUMBER,
    'STRING': STRING,
    'BINARY': BINARY,
    'NUMBER_SET': NUMBER_SET,
    'STRING_SET': STRING_SET,
    'BINARY_SET': BINARY_SET,
}
TYPES_REV = dict(((v, k) for k, v in six.iteritems(TYPES)))


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

    def __str__(self):
        return self.value

    def encode(self):
        """ Encode the binary string for Dynamo """
        return base64.b64encode(self.value).decode()

    @classmethod
    def decode(cls, value):
        """ Decode the binary string stored in Dynamo """
        return cls(base64.b64decode(value.encode()))


def encode_set(dynamizer, value):
    """ Encode a set """
    inner_value = next(iter(value))
    inner_type = dynamizer.raw_encode(inner_value)[0]
    return inner_type + 'S', [dynamizer.raw_encode(v)[1] for v in value]


class Dynamizer(object):

    """ Handles the encoding/decoding of Dynamo values """

    def __init__(self):
        self.encoders = {}
        self.register_encoder(six.text_type, lambda _, v: (STRING, v))
        self.register_encoder(six.binary_type, lambda _,
                              v: (STRING, v.decode('utf-8')))
        for t in six.integer_types:
            self.register_encoder(t, lambda _, v: (NUMBER, v))
        self.register_encoder(
            float, lambda _, v: (NUMBER, str(float_to_decimal(v))))
        # TODO: (stevearc 2014-02-25) make sure decimal is 38 point precision
        self.register_encoder(Decimal, lambda _, v: (NUMBER, str(v)))
        self.register_encoder(set, encode_set)
        self.register_encoder(Binary, lambda _, v: (BINARY, v.encode()))

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
        return dict(((k, self.encode(v)) for k, v in six.iteritems(keys)))

    def encode(self, value):
        """ Encode a value into the Dynamo dict format """
        return dict([self.raw_encode(value)])

    def decode_keys(self, keys):
        """ Run the decoder on a dict of values """
        return dict(((k, self.decode(v)) for k, v in six.iteritems(keys)))

    def decode(self, dynamo_value):
        """ Decode a dynamo value into a python value """
        type, value = list(dynamo_value.items())[0]
        if type == STRING:
            return value
        elif type == BINARY:
            return Binary.decode(value)
        elif type == NUMBER:
            return Decimal(value)
        elif type == STRING_SET:
            return set(value)
        elif type == BINARY_SET:
            return set((Binary.decode(v) for v in value))
        elif type == NUMBER_SET:
            return set((Decimal(v) for v in value))