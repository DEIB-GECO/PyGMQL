import gmql as gl
import numpy as np
import pandas as pd

input_path = "/home/luca/Scrivania/GMQL-Python/resources/metaData_repl"

bed_parser = gl.parsers.BedParser(delimiter="\t", chrPos=None, startPos=None, stopPos=None)
dataset = gl.GMQLDataset(parser=bed_parser)

dataset = dataset.load_from_path(path=input_path)