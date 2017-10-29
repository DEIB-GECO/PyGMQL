from .MetaField import MetaField
from .RegField import RegField


def SQRT(argument):
    """ Computes the square matrix of the argument

    :param argument: a dataset region field (dataset.field) or metadata (dataset['field'])
    """
    if isinstance(argument, MetaField):
        return argument._unary_expression("SQRT")
    elif isinstance(argument, RegField):
        return argument._unary_expression("SQRT")
    else:
        raise TypeError("You have to give as input a RegField (dataset.field)"
                        "or a MetaField (dataset['field']")

