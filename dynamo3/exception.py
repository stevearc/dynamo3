""" Exceptions and exception logic for DynamoDBConnection """
import sys

import botocore
import six
from pprint import pformat


class DynamoDBError(botocore.exceptions.BotoCoreError):

    """ Base error that we get back from Dynamo """
    fmt = '{Code}: {Message}\nArgs: {args}'

    def __init__(self, status_code, exc_info=None, **kwargs):
        self.exc_info = exc_info
        self.status_code = status_code
        super(DynamoDBError, self).__init__(**kwargs)

    def re_raise(self):
        """ Raise this exception with the original traceback """
        if self.exc_info is not None:
            six.reraise(type(self), self, self.exc_info[2])
        else:
            raise self


class ConditionalCheckFailedException(DynamoDBError):

    """ Raised when an item field value fails the expected value check """

    fmt = '{Code}: {Message}'

CheckFailed = ConditionalCheckFailedException


class ProvisionedThroughputExceededException(DynamoDBError):

    """ Raised when an item field value fails the expected value check """

    fmt = '{Code}: {Message}'

ThroughputException = ProvisionedThroughputExceededException

EXC = {
    'ConditionalCheckFailedException': ConditionalCheckFailedException,
    'ProvisionedThroughputExceededException': ThroughputException,
}


def translate_exception(exc, kwargs):
    """ Translate a botocore.exceptions.ClientError into a dynamo3 error """
    error = exc.response['Error']
    error.setdefault('Message', '')
    err_class = EXC.get(error['Code'], DynamoDBError)
    return err_class(exc.response['ResponseMetadata']['HTTPStatusCode'],
                     exc_info=sys.exc_info(), args=pformat(kwargs), **error)
