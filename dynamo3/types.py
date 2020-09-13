""" DynamoDB types and type logic """
from decimal import Clamped, Context, Decimal, Overflow, Underflow
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union

from typing_extensions import Literal, TypedDict

from .constants import (
    BINARY,
    BINARY_SET,
    BOOL,
    LIST,
    MAP,
    NULL,
    NUMBER,
    NUMBER_SET,
    STRING,
    STRING_SET,
)

DECIMAL_CONTEXT = Context(
    Emin=-128, Emax=126, rounding=None, prec=38, traps=[Clamped, Overflow, Underflow]
)

# Dynamo values after we have encoded them
DynamoNumber = TypedDict("DynamoNumber", {"N": Decimal})
DynamoString = TypedDict("DynamoString", {"S": str})
DynamoBinary = TypedDict("DynamoBinary", {"B": "Binary"})
DynamoSetNumber = TypedDict("DynamoSetNumber", {"NS": List[Decimal]})
DynamoSetString = TypedDict("DynamoSetString", {"SS": List[str]})
DynamoSetBinary = TypedDict("DynamoSetBinary", {"BS": List["Binary"]})
DynamoList = TypedDict(
    "DynamoList", {"L": List[Any]}  # mypy doesn't support recursion yet
)
DynamoBool = TypedDict("DynamoBool", {"BOOL": bool})
DynamoNull = TypedDict("DynamoNull", {"NULL": Literal[True]})
DynamoMap = TypedDict(
    "DynamoMap", {"M": Dict[str, Any]}  # mypy doesn't support recursion yet
)
EncodedDynamoValue = Union[
    DynamoNumber,
    DynamoString,
    DynamoBinary,
    DynamoSetNumber,
    DynamoSetString,
    DynamoSetBinary,
    DynamoList,
    DynamoBool,
    DynamoMap,
    DynamoNull,
]
EncodedDynamoObject = Dict[str, EncodedDynamoValue]


# Dynamo values decoded from the API
DecodedDynamoNumber = TypedDict("DecodedDynamoNumber", {"N": str})
DecodedDynamoBinary = TypedDict("DecodedDynamoBinary", {"B": bytes})
DecodedDynamoSetNumber = TypedDict("DecodedDynamoSetNumber", {"NS": List[str]})
DecodedDynamoSetBinary = TypedDict("DecodedDynamoSetBinary", {"BS": List[bytes]})
DecodedDynamoObject = Union[
    DecodedDynamoNumber,
    DynamoString,
    DecodedDynamoBinary,
    DecodedDynamoSetNumber,
    DynamoSetString,
    DecodedDynamoSetBinary,
    DynamoList,
    DynamoBool,
    DynamoMap,
    DynamoNull,
]

# Encoders return a tuple of (str, value)
DynamoEncoderNumber = Tuple[Literal["N"], Decimal]
DynamoEncoderString = Tuple[Literal["S"], str]
DynamoEncoderBinary = Tuple[Literal["B"], "Binary"]
DynamoEncoderSetNumber = Tuple[Literal["NS"], List[Decimal]]
DynamoEncoderSetString = Tuple[Literal["SS"], List[str]]
DynamoEncoderSetBinary = Tuple[Literal["BS"], List["Binary"]]
DynamoEncoderList = Tuple[Literal["L"], List[EncodedDynamoValue]]
DynamoEncoderBool = Tuple[Literal["BOOL"], bool]
DynamoEncoderMap = Tuple[Literal["M"], EncodedDynamoObject]
DynamoEncoderNull = Tuple[Literal["NULL"], Literal[True]]
EncoderReturn = Union[
    DynamoEncoderNumber,
    DynamoEncoderString,
    DynamoEncoderBinary,
    DynamoEncoderSetNumber,
    DynamoEncoderSetString,
    DynamoEncoderSetBinary,
    DynamoEncoderList,
    DynamoEncoderBool,
    DynamoEncoderMap,
    DynamoEncoderNull,
]

# An object in python that can be stored to DynamoDB
DynamoObject = Dict[str, Any]

# Expression types
ExpressionValueType = Any
ExpressionValuesType = Dict[str, ExpressionValueType]
ExpressionAttributeNamesType = Dict[str, str]


def float_to_decimal(f: float) -> Decimal:
    """ Convert a float to a 38-precision Decimal """
    n, d = f.as_integer_ratio()
    numerator, denominator = Decimal(n), Decimal(d)
    return DECIMAL_CONTEXT.divide(numerator, denominator)


TYPES = {
    "NUMBER": NUMBER,
    "STRING": STRING,
    "BINARY": BINARY,
    "NUMBER_SET": NUMBER_SET,
    "STRING_SET": STRING_SET,
    "BINARY_SET": BINARY_SET,
    "LIST": LIST,
    "BOOL": BOOL,
    "MAP": MAP,
    "NULL": NULL,
}
TYPES_REV = dict(((v, k) for k, v in TYPES.items()))


def is_dynamo_value(value: Any) -> bool:
    """ Returns True if the value is a Dynamo-formatted value """
    if not isinstance(value, dict) or len(value) != 1:
        return False
    subkey = next(iter(value.keys()))
    return subkey in TYPES_REV


def is_null(value: Any) -> bool:
    """ Check if a value is equivalent to null in Dynamo """
    return value is None or (isinstance(value, (set, frozenset)) and len(value) == 0)


class Binary(object):

    """ Wrap a binary string """

    def __init__(self, value: Union[str, bytes]):
        if isinstance(value, str):
            value = value.encode("utf-8")
        if not isinstance(value, bytes):
            raise TypeError("Value must be a string of binary data!")

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
        return "Binary(%r)" % self.value


def encode_set(
    dynamizer: "Dynamizer",
    value: Iterable[Union[int, float, Decimal, str, bytes, "Binary"]],
) -> EncoderReturn:
    """ Encode a set for the DynamoDB format """
    inner_value = next(iter(value))
    inner_type = dynamizer.raw_encode(inner_value)[0]
    encoded_set: Any = [dynamizer.raw_encode(v)[1] for v in value]
    set_type: Any = inner_type + "S"
    return set_type, encoded_set


def encode_list(dynamizer: "Dynamizer", value: List[Any]) -> DynamoEncoderList:
    """ Encode a list for the DynamoDB format """
    encoded_list: Any = []
    dict(map(dynamizer.raw_encode, value))
    for v in value:
        encoded_type, encoded_value = dynamizer.raw_encode(v)
        encoded_list.append(
            {
                encoded_type: encoded_value,
            }
        )
    return "L", encoded_list


def encode_dict(dynamizer: "Dynamizer", value: Any) -> DynamoEncoderMap:
    """ Encode a dict for the DynamoDB format """
    encoded_dict: Any = {}
    for k, v in value.items():
        encoded_type, encoded_value = dynamizer.raw_encode(v)
        encoded_dict[k] = {
            encoded_type: encoded_value,
        }
    return "M", encoded_dict


TagDict = TypedDict("TagDict", {"Key": str, "Value": str})


def encode_tags(tags: Dict[str, str]) -> List[TagDict]:
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def build_expression_values(
    dynamizer: "Dynamizer",
    expr_values: Optional[ExpressionValuesType],
    kwargs: ExpressionValueType,
) -> Optional[EncodedDynamoObject]:
    """ Build ExpresionAttributeValues from a value or kwargs """
    if expr_values:
        values = expr_values
        return dynamizer.encode_keys(values)
    elif kwargs:
        values = dict(((":" + k, v) for k, v in kwargs.items()))
        return dynamizer.encode_keys(values)
    return None


class Dynamizer(object):

    """ Handles the encoding/decoding of Dynamo values """

    def __init__(self):
        self.encoders = {}
        self.register_encoder(str, lambda _, v: (STRING, v))
        self.register_encoder(bytes, lambda _, v: (STRING, v.decode("utf-8")))
        self.register_encoder(int, lambda _, v: (NUMBER, str(v)))
        self.register_encoder(float, lambda _, v: (NUMBER, str(float_to_decimal(v))))
        self.register_encoder(
            Decimal,
            lambda _, v: (NUMBER, str(DECIMAL_CONTEXT.create_decimal(v))),
        )
        self.register_encoder(set, encode_set)
        self.register_encoder(frozenset, encode_set)
        self.register_encoder(Binary, lambda _, v: (BINARY, v.value))
        self.register_encoder(bool, lambda _, v: (BOOL, v))
        self.register_encoder(list, encode_list)
        self.register_encoder(dict, encode_dict)
        self.register_encoder(type(None), lambda _, v: (NULL, True))

    def register_encoder(self, type: Type, encoder: Callable) -> None:
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

    def raw_encode(self, value: Any) -> EncoderReturn:
        """ Run the encoder on a value """
        if type(value) in self.encoders:
            encoder = self.encoders[type(value)]
            return encoder(self, value)
        raise ValueError(
            "No encoder for value '%s' of type '%s'" % (value, type(value))
        )

    def encode_keys(self, keys: DynamoObject) -> EncodedDynamoObject:
        """ Run the encoder on a dict of values """
        return dict(((k, self.encode(v)) for k, v in keys.items() if not is_null(v)))

    def maybe_encode_keys(
        self, keys: Union[DynamoObject, EncodedDynamoObject]
    ) -> EncodedDynamoObject:
        """ Same as encode_keys but a no-op if already in Dynamo format """
        ret = {}
        for k, v in keys.items():
            if is_dynamo_value(v):
                return keys
            elif not is_null(v):
                ret[k] = self.encode(v)
        return ret

    def encode(self, value: Any) -> EncodedDynamoValue:
        """ Encode a value into the Dynamo dict format """
        return dict([self.raw_encode(value)])  # type: ignore

    def decode_keys(self, keys: dict) -> DynamoObject:
        """ Run the decoder on a dict of values """
        return {k: self.decode(v) for k, v in keys.items()}

    def decode(self, dynamo_value: dict) -> Optional[Any]:
        """ Decode a dynamo value into a python value """
        # mypy can't do the type refinement needed here :(
        type, value = next(iter(dynamo_value.items()))
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
            for k, v in value.items():
                decoded_dict[k] = self.decode(v)
            return decoded_dict
        elif type == NULL:
            return None
        else:
            raise TypeError("Received unrecognized type %r from dynamo" % type)
