from .BedParser import BedParser
from .NarrowPeakParser import NarrowPeakParser

string_aliases = ['string', 'char']
int_aliases = ['long', 'int', 'integer']
float_aliases = ['double', 'float']

allowed_types = ['bed', 'tab']


def get_parsing_function(type_string):
    if type_string in string_aliases:
        return str
    elif type_string in int_aliases:
        return int
    elif type_string in float_aliases:
        return float
    else:
        raise ValueError("This type is not supported")
