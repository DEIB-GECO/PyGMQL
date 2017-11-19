from ..parsers.RegionParser import RegionParser
from . import *
from ..parsers import get_type_name


def parserToXML(parser, datasetName, path):
    if not isinstance(parser, RegionParser):
        raise TypeError("parser must be a Parser. {} was given".format(type(parser)))

    result = schema_header
    result += schema_collection_template.format(datasetName)
    result += schema_dataset_type_template.format(parser.get_parser_type(), parser.get_coordinates_system())

    for f, t in zip(parser.get_ordered_attributes(), map(get_type_name, parser.get_ordered_types())):
        result += schema_field_template.format(t, f)

    result += schema_dataset_end
    result += schema_collection_end

    with open(path, "w") as f:
        f.write(result)
