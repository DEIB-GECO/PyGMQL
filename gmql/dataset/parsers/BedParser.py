from .Parser import Parser
from ..DataStructures.RegRecord import RegRecord


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
        self.otherPos = otherPos
        self.parser_name = parser_name

        # managing extra fields

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

        reg_record = RegRecord(chr=chr, start=start, stop=stop,
                               strand=strand)
        # other attributes

        for op in self.otherPos:
            v = op[2](elems[op[0]])
            reg_record.__setattr__(op[1], v)

        res = (id_record, reg_record)

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


