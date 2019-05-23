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
    1. [On Google Cloud Storage](./notebooks/03b_GWAS_Google_Cloud.ipynb): extended version of the example shown in the paper
    with more visualizations. Data reside on Google Cloud Storage.
    2. [On HDFS](notebooks/03a_GWAS_HDFS.ipynb): same example, but data reside on HDFS

## Scripts
1. [Transcriptional Interaction and Co-regulation Analyser (TICA)](): the last and most complex application example of the library. 
This query has been tested and deployed on AWS EMR. We have a script for every cell line.
    1. [GM12878](./scripts/TICA_gm12878.py)
    2. [HEPG2](./scripts/TICA_hepg2.py)
    3. [K562](./scripts/TICA_k562.py)