import gmql as gl

input_path = "/home/luca/Documenti/resources/hg_narrowPeaks_short/"

parser = gl.parsers.NarrowPeakParser()
dataset = gl.load_from_path(path=input_path, parser=parser, all_load=True)
# dataset.reg_select((dataset.chr == 'chr9') & (dataset.score < 1))
# result_gframe = dataset.materialize()
#
# print(result_gframe.regs.head())
#
# print(result_gframe.meta.head())

print("DONE")



