import gmql as gl
from gmql.dataset.GMQLDataset import  GMQLDataset
from gmql.dataset.parsers.BedParser import BedParser

bed_path = "/home/luca/Scrivania/GMQL-Python/resources/ENCSR373BIX_rep2_1_pe_bwa_biorep_filtered_hotspots.bed"

dataset = GMQLDataset()
bed_parser = BedParser()

print('starting reading bed file')
bed_dataset = dataset.load_from_path(path=bed_path, parser=bed_parser)

# Select only the lines with 'id' = 'id-2'
only_id2_lines_dataset = bed_dataset.meta_select(lambda row: row['name'] == 'id-2')

