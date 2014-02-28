""" Utilities """


def is_null(value):
    """ Check if a value is equivalent to null in Dynamo """
    return value is None or value == set() or value == frozenset()
