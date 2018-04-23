# -*- coding: utf-8 -*-

# strings to recognize as NaN
NULL = "null"
INF = "∞"
UNDEFINED = "�"
null_values = [NULL, INF, UNDEFINED]

GTF = "gtf"
TAB = "tab"

string_aliases = ['string', 'char']
int_aliases = ['long', 'int', 'integer']
float_aliases = ['double', 'float']
bool_aliases = ['bool', 'boolean']
allowed_types = ['bed', 'tab']


COORDS_ZERO_BASED = '0-based'
COORDS_ONE_BASED = '1-based'
COORDS_DEFAULT = 'default'
coordinate_systems = {COORDS_ZERO_BASED, COORDS_ONE_BASED, COORDS_DEFAULT}


def get_parsing_function(type_string):
    if type_string in string_aliases:
        return str
    elif type_string in int_aliases:
        return int
    elif type_string in float_aliases:
        return float
    elif type_string in bool_aliases:
        return bool
    else:
        raise ValueError("This type is not supported")


def get_type_name(type_class):
    if type_class == str:
        return "string"
    elif type_class == int:
        return "integer"
    elif type_class == float:
        return "double"
    elif type_class == bool:
        return 'bool'
    else:
        raise ValueError("This type is not supported")


from .RegionParser import *
from .Parsers import *
from .MetadataParser import *