# Example notebooks and scripts

In this folder you can access a set of examples and script showcasing the usage of the library in various deployment settings

## Notebooks

1. [Simple example of local execution](./notebooks/01_A_simple_example.ipynb): introductory example
2. Local and Remote computation
    1. [Minimal](./notebooks/02a_Mixing_Local_Remote_Processing_SIMPLE.ipynb):  the first application example shown in 
    the paper
    2. [Complete](./notebooks/02b_Mixing_Local_Remote_Processing_COMPLETE.ipynb): extended version of the previous application
    using a wider set of histone marks and genes
3. GWAS
    1. [Local execution](./notebooks/03a_GWAS_Local.ipynb): Simplified version of the query shown in the paper. The 
    computation is performed locally without the support of a cluster.
    1. [On Google Cloud Storage](./notebooks/03c_GWAS_Google_Cloud.ipynb): extended version of the example shown in the paper
    with more visualizations. Data reside on Google Cloud Storage. **NB: this query cannot be executed in the docker image**
    2. [On HDFS](notebooks/03b_GWAS_HDFS.ipynb): same example, but data reside on HDFS. **NB: this query cannot be executed in the docker image**

## Scripts
1. [Transcriptional Interaction and Co-regulation Analyser (TICA)](): the last and most complex application example of the library. 
This query has been tested and deployed on AWS EMR. We have a script for every cell line. **NB: this query cannot be executed in the docker image**
    1. [GM12878](./scripts/TICA_gm12878.py)
    2. [HEPG2](./scripts/TICA_hepg2.py)
    3. [K562](./scripts/TICA_k562.py)
    
## Executing the queries on HDFS
In order to run the programs which make use of a Spark cluster with an Hadoop file system it is necessary to have:

- A correctly installed Hadoop file system: you can download Hadoop from [this link]() and then follow 
[this guide](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) to setup yours
- A correctly installed Spark distribution: you can download it from 
[this link](https://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz) and then follow the instructions
at [this link](https://spark.apache.org/docs/latest/running-on-yarn.html)
- The GMQL repository data used in the workflows
    - you can download the whole set of GDM datasets used in the queries from
[this link](https://s3.us-east-2.amazonaws.com/geco-repository/geco-repository-minimal.tar.gz)
    - unpack the `tar.gz` file 
    - use `hdfs dfs put ./geco-repository hdfs:///` to put the contents of the uncompressed folder in HDFS