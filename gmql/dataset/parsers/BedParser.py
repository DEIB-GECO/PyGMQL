from .Parser import Parser, parse_strand
from ... import get_python_manager, _get_gateway
import numpy as np


class BedParser(Parser):
    """
    Generic parser for delimiter separated files
    """
    delimiter = None

    def __init__(self, chrPos, startPos,
                 stopPos, strandPos=None, otherPos=None, parser_name=None,
                 delimiter="\t"):
        """ Generic Bed Parser

        :param delimiter: delimiter of the columns of the file. Default "\t"
        :param chrPos: position of the chromosome column
        :param startPos: position of the start column
        :param stopPos: position of the stop column
        :param strandPos: if present, the position of the strand column
        :param otherPos: list of tuples of the type [(pos, attr_name, typeFun), ...]
        """
        self.delimiter = delimiter
        self.chrPos = chrPos
        self.startPos = startPos
        self.stopPos = stopPos
        self.strandPos = strandPos
        self.otherPos = getTypes(otherPos)
        self.parser_name = parser_name

        strand = -1
        if self.strandPos is not None:
            strand = self.strandPos

        otherPosJavaList = _get_gateway().jvm.java.util.ArrayList()

        if otherPos:
            for o in otherPos:
                pos = o[0]
                name = o[1]
                type = o[2]
                posJavaList = _get_gateway().jvm.java.util.ArrayList()
                posJavaList.append(str(pos))
                posJavaList.append(str(name))
                posJavaList.append(str(type))
                otherPosJavaList.append(posJavaList)

        # construction of the scala parser
        pmng = get_python_manager()
        self.gmql_parser = pmng.buildParser(self.delimiter,
                                            self.chrPos,
                                            self.startPos,
                                            self.stopPos,
                                            strand,
                                            otherPosJavaList)

    def parse_line_reg(self, id_record, line):
        elems = line.split(self.delimiter)

        chr = elems[self.chrPos]
        start = int(elems[self.startPos])
        stop = int(elems[self.stopPos])

        if self.strandPos is not None:
            s = elems[self.strandPos]
            strand = parse_strand(s)
        else:
            strand = '*'

        # reg_record = RegRecord(chr=chr, start=start, stop=stop,
        #                        strand=strand)

        reg_record = {'id_sample' : id_record,
                      'chr' : chr,
                      'start' : start,
                      'stop': stop,
                      'strand': strand}

        # other attributes
        if self.otherPos:   # only if defined
            for op in self.otherPos:
                v = op[2](elems[op[0]])
                reg_record[op[1]] = v

        res = reg_record

        return res

    def parse_line_meta(self, id_record, line):
        elems = line.split(self.delimiter)
        return id_record, (elems[0], elems[1])

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
        return types_arr[poss].tolist()

    def get_parser_name(self):
        return self.parser_name

    def get_gmql_parser(self):
        return self.gmql_parser

    def get_parser_type(self):
        return "bed"


def getTypes(otherPos):
    if otherPos:
        result = []
        for o in otherPos:
            type = o[2].lower()
            fun = str
            if type in ['string', 'char']:
                fun = str
            elif type in ['long', 'int', 'integer']:
                fun = int
            elif type in ['double', 'float']:
                fun = float
            result.append((o[0], o[1], fun))

        return result
    return None


class NarrowPeakParser(BedParser):
    """
    Parser for ENCODE NarrowPeak files.
    """
    def __init__(self):
        delimiter = '\t'
        chrPos = 0
        startPos = 1
        stopPos = 2
        strandPos = 5
        otherPos = [(3, 'name', "string"),
                    (4, 'score', "float"),
                    (6, 'signalValue', "float"),
                    (7, 'pValue', "float"),
                    (8, 'qValue', "float"),
                    (9, 'peak', "float")]

        super().__init__(parser_name='NarrowPeakParser', delimiter=delimiter,
                         chrPos=chrPos, startPos=startPos,
                         stopPos=stopPos, strandPos=strandPos, otherPos=otherPos)


class Bed10(BedParser):
    """ Standard Full BED Parser of 10 Columns
    """
    def __init__(self):
        delimiter = "\t"
        chrPos = 0
        startPos = 1
        stopPos = 2
        strandPos = 5
        otherPos = [
            (3, "3", "string"),
            (4, "4", "double"),
            (6, "6", "double"),
            (7, "7", "double"),
            (8, "8", "double"),
            (9, "9", "double")
        ]
        super().__init__(parser_name='Bed10', delimiter=delimiter,
                         chrPos=chrPos, startPos=startPos,
                         stopPos=stopPos, strandPos=strandPos, otherPos=otherPos)


class ANNParser(BedParser):
    """ Annotation Parser (6 columns)
    """
    def __init__(self):
        super().__init__(chrPos=0, startPos=1, stopPos=2, strandPos=5,
                         otherPos=[(3, "name", "string"), (4, "score", "double")],
                         delimiter="\t", parser_name="AnnotationParser")


# TODO: ADD THE REST OF THE PARSERS