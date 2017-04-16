import gmql as gl

# loading of a dataset
data_path = "/home/luca/Documenti/resources/hg_narrowPeaks"
output_path = "/home/luca/Documenti/resources/result"

parser = gl.parsers.NarrowPeakParser()

data = gl.GMQLDataset(parser=parser) \
    .load_from_path(path=data_path)

data.show_info()

# apply an operation

condition = (gl.MetaField("cell") == 'K562') & (gl.MetaField("cell_tissue") == 'blood') & (gl.MetaField("cell_sex") == 'F')

filtered_data = data.meta_select(predicate=condition) # test

filtered_data = filtered_data.materialize(output_path=output_path)

