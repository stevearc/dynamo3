""" Exceptions and exception logic for DynamoDBConnection """
import botocore
from pprint import pformat


class DynamoDBError(botocore.exceptions.BotoCoreError):

    """ Base error that we get back from Dynamo """
    fmt = '{Code}: {Message}\nArgs: {args}'

    def __init__(self, status_code, **kwargs):
        self.status_code = status_code
        super(DynamoDBError, self).__init__(**kwargs)


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


def raise_if_error(kwargs, response, data):
    """ Check a response and raise the correct exception if needed """
    if 'Errors' in data:
        error = data['Errors'][0]
        error.setdefault('Message', '')
        err_class = EXC.get(error['Code'], DynamoDBError)
        raise err_class(response.status_code, args=pformat(kwargs),
                        **error)
    response.raise_for_status()
