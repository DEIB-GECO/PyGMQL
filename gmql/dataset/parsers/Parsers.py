from .RegionParser import RegionParser
from . import COORDS_DEFAULT
from . import TAB


class BedParser(RegionParser):
    """ Standard Full BED Parser of 10 Columns
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=5,
                         otherPos=[(3, "3", "string"),
                                   (4, "4", 'double'),
                                   (6, "6", 'double'),
                                   (7, "7", 'double'),
                                   (8, "8", 'double'),
                                   (9, "9", "double")],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class ANNParser(RegionParser):
    """ Annotation Parser, 6 columns
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=5,
                         otherPos=[(3, "name", "string"),
                                   (4, "score", 'double')],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class BroadProjParser(RegionParser):
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=None,
                         otherPos=[(3, "name", "string")],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class BasicParser(RegionParser):
    """ Parser for Chr, Start, Stop only (no Strand)
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=None,
                         otherPos=None,
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class NarrowPeakParser(RegionParser):
    """ Narrow Peaks Parser. 10 columns
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=5,
                         otherPos=[(3, "name", "string"),
                                   (4, "score", 'double'),
                                   (6, "signalValue", 'double'),
                                   (7, "pValue", 'double'),
                                   (8, "qValue", 'double'),
                                   (9, "peak", "double")],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class RnaSeqParser(RegionParser):
    """ Standard Full BED Parser of 10 Columns
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=3,
                         otherPos=[(4, "name", "string"),
                                   (5, "score", 'double')],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)


class BedScoreParser(RegionParser):
    """ Standard Full BED Parser of 10 Columns
    """
    def __init__(self):
        super().__init__(chrPos=0,
                         startPos=1,
                         stopPos=2,
                         strandPos=None,
                         otherPos=[(3, "score", 'double')],
                         delimiter="\t",
                         coordinate_system=COORDS_DEFAULT,
                         schema_format=TAB)

