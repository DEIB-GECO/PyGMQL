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
    else:
        raise ValueError("This type is not supported")


def get_type_name(type):
    if type == str:
        return "string"
    elif type == int:
        return "integer"
    elif type == float:
        return "double"
    else:
        raise ValueError("This type is not supported")


from .RegionParser import *
from .Parsers import *
from .MetadataParser import *