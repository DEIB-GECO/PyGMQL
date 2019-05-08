from ...managers import get_python_manager, get_gateway
from ...scala_wrapper import none, Some
from . import coordinate_systems, get_parsing_function, null_values, GTF
import numpy as np
import pandas as pd
import re


class RegionParser:
    def __init__(self, gmql_parser=None, chrPos=None, startPos=None, stopPos=None,
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

        if isinstance(parser_name, str):
            self.parser_name = parser_name
        else:
            raise TypeError("Parser name must be a string")

        if gmql_parser is None:
            if not isinstance(delimiter, str):
                raise ValueError("delimiter must be a string")
            if not (isinstance(chrPos, int) and chrPos >= 0):
                raise ValueError("Chromosome position must be >=0")
            if not (isinstance(startPos, int) and startPos >= 0):
                raise ValueError("Start position must be >=0")
            if not (isinstance(stopPos, int) and stopPos >= 0):
                raise ValueError("Stop position must be >=0")
            if isinstance(strandPos, int) and strandPos >= 0:
                strandGmql = Some(strandPos)
            elif strandPos is None:
                strandGmql = none()
            else:
                raise ValueError("Strand position must be >= 0")
            if not isinstance(coordinate_system, str):
                raise TypeError("Coordinate system must be a string")
            if coordinate_system not in coordinate_systems:
                raise ValueError("{} is not a valid coordinate system".format(coordinate_system))
            if not isinstance(schema_format, str):
                raise TypeError("Schema Format must be a string")
            if isinstance(otherPos, list):
                otherPosGmql = Some(convert_to_gmql(otherPos))
            else:
                otherPosGmql = none()

            pmg = get_python_manager()

            self.gmql_parser = pmg.buildParser(delimiter, chrPos, startPos,
                                               stopPos, strandGmql, otherPosGmql,
                                               schema_format, coordinate_system)
        else:
            self.gmql_parser = gmql_parser

    @staticmethod
    def from_schema_file(schema_file):
        pmg = get_python_manager()
        gmql_parser = pmg.getParserFromPath(schema_file)
        return RegionParser(gmql_parser)

    @property
    def delimiter(self):
        return self.gmql_parser.delimiter()

    @property
    def chrPos(self):
        return self.gmql_parser.chrPos()

    @property
    def startPos(self):
        return self.gmql_parser.startPos()

    @property
    def stopPos(self):
        return self.gmql_parser.stopPos()

    @property
    def strandPos(self):
        if self.gmql_parser.strandPos().isDefined():
            return self.gmql_parser.strandPos().get()
        else:
            return None

    @property
    def otherPos(self):
        res = []
        if self.gmql_parser.otherPos().isDefined():
            for poss, sch in zip(self.gmql_parser.otherPos().get(), self.gmql_parser.getSchema()):
                pos = poss._1()
                attr_name = sch._1()
                typeFun = get_parsing_function(poss._2().toString().lower())
                res.append((pos, attr_name, typeFun))
        return res

    def get_coordinates_system(self):
        return self.gmql_parser.coordinateSystem().toString()

    def get_parser_type(self):
        return self.gmql_parser.parsingType().toString()

    def get_gmql_parser(self):
        """ Gets the Scala implementation of the parser

        :return: a Java Object
        """
        return self.gmql_parser

    @staticmethod
    def parse_strand(strand):
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
        if self.get_parser_type().lower() == GTF.lower():
            res = self._parse_gtf_regions(path)
        else:
            res = self._parse_tab_regions(path)
        return res

    def _parse_tab_regions(self, path):
        types = self.get_name_type_dict()
        if "strand" in types.keys():
            types.pop("strand")
        fo = open(path)
        df = pd.read_csv(filepath_or_buffer=fo, na_values=null_values,
                         header=None,
                         names=self.get_ordered_attributes(),
                         dtype=types,
                         sep=self.delimiter,
                         converters={'strand': self.parse_strand})
        fo.close()
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
        fo = open(path)
        df = pd.read_csv(filepath_or_buffer=fo, sep=self.delimiter,
                         na_values=null_values,
                         names=actual_attributes,
                         dtype=types,
                         converters={'strand': self.parse_strand})
        fo.close()
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
        for i, a in enumerate(attrs):
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
            raise TypeError("Type of region field must be a string")
        if isinstance(tpos[0], int) and isinstance(tpos[1], str):
            return tpos[0], tpos[1], fun
        else:
            raise TypeError("Position must be integer and name of field must be a string")


def convert_to_gmql(otherPos):
    otherPosJavaList = get_gateway().jvm.java.util.ArrayList()
    for tpos in otherPos:
        posJavaList = get_gateway().jvm.java.util.ArrayList()
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
