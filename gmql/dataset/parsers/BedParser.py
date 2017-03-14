from .Parser import Parser
from pybedtools import BedTool


class BedParser(Parser):

    header = ['chrom', 'chromStart', 'chromEnd', 'name', 'score', 'strand', 'thickStart', 'thickEnd',
              'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']

    def parse_line(self, line):
        elems = line.split("\t")
        n_fields = len(elems)
        dict_row = {}
        for i in range(n_fields):
            dict_row[self.header[i]] = elems[i]
        return dict_row

    def get_attributes(self):
        return self.header


def parse_to_dataframe(self, file_path):
    bed_file = BedTool(file_path)
    df = bed_file.to_dataframe()
    return df
