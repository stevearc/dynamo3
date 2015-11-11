""" Utility methods """


def snake_to_camel(name):
    """ Convert snake_case to CamelCase """
    return ''.join([piece.capitalize() for piece in name.split('_')])


def dry_call(command, kwargs):
    """ helper to create a dry_run call tuple """
    return (snake_to_camel(command), kwargs)
