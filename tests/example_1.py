from gmql.dataset.GMQLDataset import GMQLDataset
from gmql.dataset.parsers.BedParser import BedParser

bed_path = "/home/luca/Scrivania/GMQL-Python/resources/"
bed_parser = BedParser()

dataset = GMQLDataset(parser=bed_parser)

print('starting reading bed file')
bed_dataset = dataset.load_from_path(path=bed_path)

# Select only the lines with 'id' = 'id-2'
only_id2_lines_dataset = bed_dataset.meta_select(lambda row: row['name'] == 'id-2')

# printout a snippet of the dataset (the first 5 elements)
df = only_id2_lines_dataset.get_snippet_dataframe(5)

print(df)