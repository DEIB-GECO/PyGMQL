"""
    GENOMETRIC MAP
    MAP is a non-symmetric operator over two datasets, respectively called reference and experiment.
    The operation computes, for each sample in the experiment dataset, aggregates over the values
    of the experiment regions that intersect with a region in a reference sample, for each region 
    of each sample in the reference dataset; we say that experiment regions are mapped to the reference
    regions. The number of generated output samples is the Cartesian product of the samples in the two
    input datasets; each output sample has the same regions as the related input reference sample,
    with their attributes and values, plus the attributes computed as aggregates over experiment region
    values. Output sample metadata are the union of the related input sample metadata, whose attribute
    names are prefixed with their input dataset name. 
    For each reference sample, the MAP operation produces a matrix like structure, called genomic space,
    where each experiment sample is associated with a row, each reference region with a column, 
    and each matrix row is a vector of numbers - the aggregates computed during MAP execution. 
    When the features of the reference regions are unknown, the MAP helps in extracting the most 
    interesting regions out of many candidates. 

"""

def genometric_map(reference, experiment):
    pass