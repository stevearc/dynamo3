""" Utility methods """


def snake_to_camel(name):
    """ Convert snake_case to CamelCase """
    return "".join([piece.capitalize() for piece in name.split("_")])
