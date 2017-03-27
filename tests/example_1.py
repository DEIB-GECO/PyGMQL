import gmql as gl

path = "/home/luca/Scrivania/GMQL-Python/resources/np_single"

parser = gl.parsers.NarrowPeakParser()

dataset = gl.GMQLDataset(parser=parser)
dataset = dataset.load_from_path(path)

print("Metadata")
print(dataset.meta_dataset)

print("Regions")
print(dataset.get_reg_attributes())
