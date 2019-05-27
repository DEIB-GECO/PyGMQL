# The script: 
# (a) reads TSS from GENCODE, 
# (b) defines promoter regions as 200 bases upstream and 500 bases downstream the TSS ,
# (c) reads ENCODE narrow peaks for TF (by excluding samples which are not of good quality and which are histonic modifications), for a given 'cell_line' 
# (d) creates a one-base region around their peaks,
# (e) associates exactly one sample to each TF by grouping by experiment target and collecting all peaks of replicates, 
# (f) for each TF samples, finds TF peaks which overlap with some promoter region, 
# (g) for all pairs of TFs, joins the regions extracted before which are at minimum distance and within 700 pairs, 
# (g) materializes the result; the 'all_load=False' parameter prevents the client to load results into memory.-

import gmql as gl
gl.set_master("yarn")

gmql_repository = "hdfs://"
my_out = gmql_repository + "/output_directory/"

narrow_ds = "/HG19_ENCODE_NARROW"
annotation_ds = "/HG19_ANNOTATION_GENCODE"
cell_line = "GM12878"
#cell_line = "K562"
#cell_line = "HepG2"

tss = gl.load_from_path(gmql_repository+ annotation_ds)

tss = tss[(tss['annotation_type'] == 'gene') & (tss['release_version'] == '19') ]
tss = tss.reg_select(tss.gene_type=="protein_coding").project()

proms = tss.reg_project(new_field_dict={'start': tss.start - 500, 'stop': tss.stop + 200}).project()

encode_narrow = gl.load_from_path(gmql_repository + narrow_ds)
tfbs_raw = encode_narrow[(encode_narrow['assay'] == "ChIP-seq") & (encode_narrow['biosample_term_name'] == cell_line) & \
                                                    (encode_narrow['output_type'] == "optimal idr thresholded peaks") & \
                         ~(encode_narrow['audit_not_compliant'].isin(["antibody not characterized to standard",
                                                                      "control low read depth",
                                                                      "insufficient read depth",
                                                                      "insufficient replicate concordance",
                                                                      "partially characterized antibody", 
                                                                      "poor library complexity", 
                                                                      "severe bottlenecking"])) & \
                        ~(encode_narrow['experiment_target'].isin(['H3K27ac-human',
                                                                   'H3K4me3-human',
                                                                   'H3K36me3-human',
                                                                   'H3K4me1-human',
                                                                   'H3K4me2-human',
                                                                   'H3K4me3-human',
                                                                   'H3K79me2-human',
                                                                   'H4K20me1-human']))]

# all of them are not stranded
narrow_peaks = tfbs_raw.reg_project(new_field_dict={
    'left': tfbs_raw.left + tfbs_raw.peak,
    'right': tfbs_raw.left + tfbs_raw.peak + 1
})

cov = narrow_peaks.normal_cover(1, "ANY",groupBy=["experiment_target"]).project()

# for each tfbs
tfbs_in_prom = cov.join(proms,genometric_predicate=[gl.DLE(0)],output="left")

pairs = tfbs_in_prom.join(tfbs_in_prom, genometric_predicate=[gl.DLE(700), gl.MD(1)],output="contig")

pairs_dist = pairs.project(new_field_dict={ "distance" : pairs.right - pairs.left })
pairs_dist.materialize(output_path=my_out + '/pairs_distribution_' + cell_line + '/', 
                       all_load=False)
