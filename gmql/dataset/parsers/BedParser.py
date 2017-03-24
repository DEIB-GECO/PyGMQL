from .Parser import Parser

class BedParser(Parser):

    header = ['chrom', 'chromStart', 'chromEnd', 'name', 'score', 'strand', 'thickStart', 'thickEnd',
              'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
    types = [str, int, int, str, float]

    delimiter = None

    def __init__(self, delimiter, chrPos, startPos, stopPos, strandPos=None, otherPos=None):
        """
        Generic Bed Parser
        :param delimiter:
        :param chrPos:
        :param startPos:
        :param stopPos:
        :param strandPos:
        :param otherPos:
        """
        self.delimiter = delimiter
        self.chrPos = chrPos
        self.startPos = startPos
        self.stopPos = startPos
        self.strandPos = strandPos

        # managing extra fields

    def parse_line_reg(self, id_record, line):
        elems = line.split(self.delimiter)

        return None

    def parse_line_meta(self, id_record, line):
        elems = line.split(self.delimiter)
        return {"id_sample": id_record, elems[0]: elems[1]}

    def get_attributes(self):
        return self.header
