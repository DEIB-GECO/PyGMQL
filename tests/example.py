import gmql as gl

# loading of a dataset
data_path = "/home/luca/Documenti/resources/hg_narrowPeaks"
parser = gl.parsers.NarrowPeakParser()

data = gl.GMQLDataset(parser=parser)\
                .load_from_path(path=data_path)

data.show_info()


