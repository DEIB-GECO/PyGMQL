import gmql as gl

# location of the datasets
coords_path = "/home/luca/Documenti/TADs/Data/GMQLFormat/coordinates"
cell_lines_path = "/home/luca/Documenti/TADs/Data/GMQLFormat/cell_lines"
# where we want to save our result
output_path = "./Results/"

# parser for the coordinates
coords_parser = gl.parsers.BedParser(parser_name="coordinates_parser",
                                     chrPos=0, strandPos=1, startPos=2, stopPos=3,
                                     delimiter="\t", otherPos=[(4,"gene","string")])

# parser for the cell_lines
cell_lines_parser = gl.parsers.BedParser(parser_name="cell_lines_parser",
                                         chrPos=0, strandPos=3, startPos=1, stopPos=2,
                                         delimiter="\t")

# loading the coordinates dataset
genes_coords = gl.GMQLDataset(parser=coords_parser).load_from_path(coords_path)
# loading the cell lines dataset
cell_lines = gl.GMQLDataset(parser=cell_lines_parser).load_from_path(cell_lines_path)

# take the IMR90 TADs
imr90 = cell_lines.meta_select(predicate=cell_lines.MetaField("cell") == 'IMR90')

# take the GM12878 TADs
gm12878 = cell_lines.meta_select(predicate=cell_lines.MetaField("cell") == 'GM12878')

# compute the pairs
pairs = genes_coords.join(experiment=genes_coords,
                            genometric_predicate=[gl.DLE(180000)],
                            output="CONTIG")

# do the mapping between the pairs and the TADs locations
mapping_temp = pairs.map(experiment=imr90,refName="pairs", expName="imr90")
mapping = mapping_temp.map(experiment=gm12878, refName="mpt", expName="gm12878")

m2 = mapping.materialize("./Results/mapping_imr90_gm12878/")