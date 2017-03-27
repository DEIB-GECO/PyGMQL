from .BedParser import BedParser


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
        otherPos = [(3, 'name', str),
                    (4, 'score', float),
                    (6, 'signalValue', float),
                    (7, 'pValue', float),
                    (8, 'qValue', float),
                    (9, 'peak', float)]
        super().__init__(delimiter, chrPos, startPos, stopPos, strandPos, otherPos)


