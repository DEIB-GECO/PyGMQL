import gmql as gl

input_path = "/home/luca/Documenti/resources/hg_narrowPeaks_short/"

parser = gl.parsers.NarrowPeakParser()

gframe = gl.load_from_path(path=input_path, parser=parser,
                           )

print("DONE")



