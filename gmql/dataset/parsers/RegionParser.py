from ... import get_python_manager, none, Some, _get_gateway
from . import coordinate_systems, get_parsing_function, null_values, GTF
import numpy as np
import pandas as pd
import re


class RegionParser:
    def __init__(self, chrPos, startPos, stopPos,
                 strandPos=None, otherPos=None, delimiter="\t",
                 coordinate_system='0-based', schema_format="del",
                 parser_name="parser"):
        """ Creates a custom region dataset

        :param chrPos: position of the chromosome column
        :param startPos: position of the start column
        :param stopPos: position of the stop column
        :param strandPos: (optional) position of the strand column. Default is None
        :param otherPos: (optional) list of tuples of the type [(pos, attr_name, typeFun), ...]. Default is None
        :param delimiter: (optional) delimiter of the columns of the file. Default "\t"
        :param coordinate_system: can be {'0-based', '1-based', 'default'}. Default is '0-based'
        :param schema_format: (optional) type of file. Can be {'tab', 'gtf', 'vcf', 'del'}. Default is 'del'
        :param parser_name: (optional) name of the parser. Default is 'parser'
        """
        if isinstance(delimiter, str):
            self.delimiter = delimiter
        if isinstance(chrPos, int) and chrPos >= 0:
            self.chrPos = chrPos
        else:
            raise ValueError("Chromosome position must be >=0")
        if isinstance(startPos, int) and startPos >= 0:
            self.startPos = startPos
        else:
            raise ValueError("Start position must be >=0")
        if isinstance(stopPos, int) and stopPos >= 0:
            self.stopPos = stopPos
        else:
            raise ValueError("Stop position must be >=0")
        if isinstance(strandPos, int) and strandPos >= 0:
            self.strandPos = strandPos
            strandGmql = Some(strandPos)
        elif strandPos is None:
            self.strandPos = None
            strandGmql = none()
        else:
            raise ValueError("Strand position must be >= 0")
        if isinstance(parser_name, str):
            self.parser_name = parser_name
        else:
            raise TypeError("Parser name must be a string")
        if isinstance(coordinate_system, str):
            if coordinate_system in coordinate_systems:
                self.coordinate_system = coordinate_system
            else:
                raise ValueError("{} is not a valid coordinate system".format(coordinate_system))
        else:
            raise TypeError("Coordinate system must be a string")
        if isinstance(schema_format, str):
            self.schema_format = schema_format
        else:
            raise TypeError("Schema Format must be a string")

        if isinstance(otherPos, list):
            self.otherPos = convert_otherPos(otherPos)
            otherPosGmql = Some(convert_to_gmql(otherPos))
        else:
            otherPosGmql = none()
            self.otherPos = None

        pmg = get_python_manager()

        self.gmql_parser = pmg.buildParser(delimiter, self.chrPos, self.startPos,
                                           self.stopPos, strandGmql, otherPosGmql,
                                           self.schema_format, self.coordinate_system)

    def get_coordinates_system(self):
        return self.coordinate_system

    def get_parser_type(self):
        return self.schema_format

    def get_gmql_parser(self):
        """ Gets the Scala implementation of the parser

        :return: a Java Object
        """
        return self.gmql_parser

    def parse_strand(self, strand):
        """ Defines how to parse the strand column

        :param strand: a string representing the strand
        :return: the parsed result
        """
        if strand in ['+', '-', '*']:
            return strand
        else:
            return '*'

    def parse_regions(self, path):
        """ Given a file path, it loads it into memory as a Pandas dataframe

        :param path: file path
        :return: a Pandas Dataframe
        """
        if self.schema_format.lower() == GTF.lower():
            res = self._parse_gtf_regions(path)
        else:
            res = self._parse_tab_regions(path)
        return res

    def _parse_tab_regions(self, path):
        types = self.get_name_type_dict()
        if "strand" in types.keys():
            types.pop("strand")
        df = pd.read_csv(filepath_or_buffer=open(path), na_values=null_values,
                         header=None,
                         names=self.get_ordered_attributes(),
                         dtype=types,
                         sep=self.delimiter,
                         converters={'strand': self.parse_strand})
        return df

    def _parse_gtf_regions(self, path):
        def split_attributes(attrs):
            res = {}
            attr_splits = re.split(";\s*", attrs)
            for a in attr_splits:
                if len(a) > 0:
                    splits = re.split("\"\s*", a)
                    #             print(splits)
                    attr_name = splits[0].strip()
                    attr_value = splits[1].strip()
                    res[attr_name] = attr_value
            return res
        actual_attributes = self.get_ordered_attributes()[:8] + ['attributes']
        types = self.get_name_type_dict()
        types.pop("strand")
        df = pd.read_csv(filepath_or_buffer=open(path), sep=self.delimiter,
                         na_values=null_values,
                         names=actual_attributes,
                         dtype=types,
                         converters={'strand': self.parse_strand})
        df = pd.concat([df, pd.DataFrame(df.attributes.map(split_attributes).tolist())], axis=1)\
            .drop("attributes", axis=1)
        return df

    def get_attributes(self):
        """ Returns the unordered list of attributes

        :return: list of strings
        """
        attr = ['chr', 'start', 'stop']
        if self.strandPos is not None:
            attr.append('strand')
        if self.otherPos:
            for i, o in enumerate(self.otherPos):
                attr.append(o[1])

        return attr

    def get_ordered_attributes(self):
        """ Returns the ordered list of attributes

        :return: list of strings
        """
        attrs = self.get_attributes()
        attr_arr = np.array(attrs)
        poss = [self.chrPos, self.startPos, self.stopPos]
        if self.strandPos is not None:
            poss.append(self.strandPos)
        if self.otherPos:
            for o in self.otherPos:
                poss.append(o[0])

        idx_sort = np.array(poss).argsort()
        return attr_arr[idx_sort].tolist()

    def get_types(self):
        """ Returns the unordered list of data types

        :return: list of data types
        """
        types = [str, int, int]
        if self.strandPos is not None:
            types.append(str)
        if self.otherPos:
            for o in self.otherPos:
                types.append(o[2])

        return types

    def get_name_type_dict(self):
        """ Returns a dictionary of the type
        {'column_name': data_type, ...}

        :return: dict
        """
        attrs = self.get_attributes()
        types = self.get_types()
        d = dict()
        for i,a in enumerate(attrs):
            d[a] = types[i]

        return d

    def get_ordered_types(self):
        """ Returns the ordered list of data types

        :return: list of data types
        """
        types = self.get_types()
        types_arr = np.array(types)
        poss = [self.chrPos, self.startPos, self.stopPos]
        if self.strandPos is not None:
            poss.append(self.strandPos)
        if self.otherPos:
            for o in self.otherPos:
                poss.append(o[0])
        idx_sort = np.array(poss).argsort()
        return types_arr[idx_sort].tolist()


def convert_otherPos(otherPos):
    # print(otherPos)
    return list(map(_to_parsing_function, otherPos))


def _to_parsing_function(tpos):
    if len(tpos) != 3:
        raise ValueError("Position tuple has wrong number of parameters")
    else:
        if isinstance(tpos[2], str):
            fun = get_parsing_function(tpos[2].lower())
        else:
            TypeError("Type of region field must be a string")
        if isinstance(tpos[0], int) and isinstance(tpos[1], str):
            return tpos[0], tpos[1], fun
        else:
            raise TypeError("Position must be integer and name of field must be a string")


def convert_to_gmql(otherPos):
    otherPosJavaList = _get_gateway().jvm.java.util.ArrayList()
    for tpos in otherPos:
        posJavaList = _get_gateway().jvm.java.util.ArrayList()
        if len(tpos) != 3:
            raise ValueError("Position tuple has wrong number of parameters")
        else:
            if isinstance(tpos[0], int) and tpos[0] >= 0:
                posJavaList.append(str(tpos[0]))
            else:
                raise ValueError("Position must be >= 0")
            if isinstance(tpos[1], str):
                posJavaList.append(tpos[1])
            else:
                raise TypeError("Name of the field must be a string")
            if isinstance(tpos[2], str):
                get_parsing_function(tpos[2])
                posJavaList.append(tpos[2])
        otherPosJavaList.append(posJavaList)
    return otherPosJavaList
