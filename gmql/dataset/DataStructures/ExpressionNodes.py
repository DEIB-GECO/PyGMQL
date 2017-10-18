from .MetaField import MetaField
from .RegField import RegField


def SQRT(argument):
    """ Computes the square matrix of the argument

    :param argument: a dataset region field (dataset.field) or metadata (dataset['field'])
    """
    if isinstance(argument, MetaField):
        new_name = "SQRT({})".format(argument.name)
        new_node = argument.exp_build.getUnaryMetaExpression(argument.meNode, "SQRT")
        result = MetaField(name=new_name, index=argument.index, meNode=new_node)
    elif isinstance(argument, RegField):
        new_name = "SQRT({})".format(argument.name)
        new_node = argument.exp_build.getUnaryRegionExpression(argument.reNode, "SQRT")
        result = RegField(name=new_name, index=argument.index, reNode=new_node)
    else:
        raise TypeError("You have to give as input a RegField (dataset.field)"
                        "or a MetaField (dataset['field']")
    return result

