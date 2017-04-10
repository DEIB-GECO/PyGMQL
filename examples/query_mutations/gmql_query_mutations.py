import gmql as gl
from gmql.expressions import *

# We can use different parsers
gwas_parser = gl.parsers.GWASparser()
encode_parser = gl.parsers.ENCODEparser()

# We load the dataset we need
gwas_dataset = gl.GMQLDataset().load("gwas")
encode_dataset = gl.GMQLDataset().load("encode_hg19")

# When the datasets are firstly loaded, all the metadata are
# collected in memory and stored in a pandas dataframe. 
# This enables the user to do an initial filtering
# of the properties that he is interested in.
# Due to the fact that we use a pandas dataframe we are able 
# to do arbitrary filtering through lambda functions.

# This call selects all the samples that have as
# experiment target  'H3K27ac−human', collects 
# all the id_samples of the selected samples
# and send them to the GMQL engine in order to  initially 
# filter the dataset
Ac = encode_dataset.meta_select(lambda meta: meta.experiment_target == 'H3K27ac−human')

# hmm...I think this can be improved...
project_arg = {
  "peak": ADD(DIV("RIGHT",2),DIV("LEFT",2))             
}
peaked = Ac.reg_project(new_attributes=project_arg)

# hmm...I think this can be improved...
project_arg = {
    "LEFT" : SUB("peak",1500),                          
    "RIGHT": ADD("peak",1500)
}
large = peaked.reg_project(new_attributes=project_arg)

# merge replicats together
rep = large.cover(groupby=['biosample_term_name'])

# find the cell-line specific enhancers
S = rep.cover(minAcc=1, maxAcc=2)

# convention: the GMQLDataset calling map is the 
# reference and the paramter of the function is the experiment
RepCount = rep.map(S)       
CSE = RepCount.reg_select(['count_REP_S','GT',0])

# find the mutation occurring in those enhancers
M = CSE.map(gwas_dataset, {'bag' : BAG("trait")})
N = M.reg_select(['count_Z_GWAS','GT',0])
P = N.meta_project(['count_Z_GWAS','bag'])

# materialization in memory <---- notice that here we collect 
# directly in the program for future analysis. 
# The result is anyway stored in the HDFS for future access
materialized_P = P.materialize(filename="test_final")
