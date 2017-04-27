from .Parser import Parser
from ..DataStructures.RegRecord import RegRecord
from ... import get_python_manager, get_gateway


class BedParser(Parser):
    """
    Generic parser for delimiter separated files
    """
    delimiter = None

    def __init__(self, parser_name, delimiter, chrPos, startPos,
                 stopPos, strandPos=None, otherPos=None):
        """
        Generic Bed Parser
        :param delimiter: delimiter of the columns of the file
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

        otherPosJavaList = get_gateway().jvm.java.util.ArrayList()

        if otherPos:
            for o in otherPos:
                pos = o[0]
                name = o[1]
                type = o[2]
                posJavaList = get_gateway().jvm.java.util.ArrayList()
                posJavaList.append(pos)
                posJavaList.append(name)
                posJavaList.append(type)
                otherPosJavaList.append(posJavaList)

        # construction of the scala parser
        self.gmql_parser = get_python_manager().buildParser(self.delimiter,
                                                            self.chrPos,
                                                            self.startPos,
                                                            strand,
                                                            otherPosJavaList)


    def parse_line_reg(self, id_record, line):
        elems = line.split(self.delimiter)

        chr = elems[self.chrPos]
        start = int(elems[self.startPos])
        stop = int(elems[self.stopPos])

        if self.strandPos is not None:
            s = elems[self.strandPos]
            if s in ['+', '-', '*']:
                strand = s
            else:
                strand = '*'
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

        for op in self.otherPos:
            v = op[2](elems[op[0]])
            reg_record[op[1]] = v

        res = reg_record

        return res

    def parse_line_meta(self, id_record, line):
        elems = line.split(self.delimiter)
        return id_record, (elems[0], elems[1])

    def get_attributes(self):
        attr = ['chr', 'start', 'stop', 'strand']

        for i, o in enumerate(self.otherPos):
            attr.append(o[1])

        return attr

    def get_parser_name(self):
        return self.parser_name


def getTypes(otherPos):
    if otherPos:
        result = []
        for o in otherPos:
            type = o[2]
            fun = str
            if type == 'string':
                fun = str
            elif type == 'long':
                fun = int
            elif type == 'char':
                fun = str
            elif type == 'double':
                fun = float
            elif type == 'integer':
                fun = int
            result.append((o[0], o[1], fun))

        return result
    return None



