from ... import get_python_manager, none, Some, _get_gateway
from . import coordinate_systems, get_parsing_function, NULL, GTF
import numpy as np
import pandas as pd
import re


class RegionParser:
    def __init__(self, chrPos, startPos, stopPos,
                 strandPos=None, otherPos=None, delimiter="\t",
                 coordinate_system='0-based', schema_format="del",
                 parser_name="parser"):
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

        pmg = get_python_manager()

        self.gmql_parser = pmg.buildParser(delimiter, self.chrPos, self.startPos,
                                           self.stopPos, strandGmql, otherPosGmql,
                                           self.schema_format, self.coordinate_system)

    def get_gmql_parser(self):
        return self.gmql_parser

    def parse_strand(self, strand):
        if strand in ['+', '-', '*']:
            return strand
        else:
            return '*'

    def parse_regions(self, path):
        if self.schema_format.lower() == GTF.lower():
            res = self._parse_gtf_regions(path)
        else:
            res = self._parse_tab_regions(path)
        return res

    def _parse_tab_regions(self, path):
        df = pd.read_csv(filepath_or_buffer=path, na_values=NULL,
                         header=None,
                         names=self.get_ordered_attributes(),
                         dtype=self.get_name_type_dict(),
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
        df = pd.read_csv(filepath_or_buffer=path, sep=self.delimiter,
                         na_values=NULL,
                         names=actual_attributes,
                         dtype=self.get_name_type_dict(),
                         converters={'strand': self.parse_strand})
        df = pd.concat([df, pd.DataFrame(df.attributes.map(split_attributes).tolist())], axis=1)\
            .drop("attributes", axis=1)
        return df


    def get_attributes(self):
        attr = ['chr', 'start', 'stop']
        if self.strandPos is not None:
            attr.append('strand')
        if self.otherPos:
            for i, o in enumerate(self.otherPos):
                attr.append(o[1])

        return attr

    def get_ordered_attributes(self):
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
        types = [str, int, int]
        if self.strandPos is not None:
            types.append(str)
        if self.otherPos:
            for o in self.otherPos:
                types.append(o[2])

        return types

    def get_name_type_dict(self):
        attrs = self.get_attributes()
        types = self.get_types()
        d = dict()
        for i,a in enumerate(attrs):
            d[a] = types[i]

        return d

    def get_ordered_types(self):
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


class BedParser(RegionParser):
    def __init__(self):
        pass
