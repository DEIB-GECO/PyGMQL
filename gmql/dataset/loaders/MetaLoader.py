import pyspark
import pandas as pd
from tqdm import tqdm
import logging

"""
    Loading functons for Metadata
"""

logger = logging.getLogger('gmql_logger')

inputFormatClass = 'it.polimi.genomics.spark.implementation.loaders.Loaders$CombineTextFileWithPathInputFormat'
keyFormatClass = 'org.apache.hadoop.io.LongWritable'
valueFormatClass = 'org.apache.hadoop.io.Text'


def load_meta_from_path(path, parser):
    conf = {"textinputformat.record.delimiter": "\n",
            "mapreduce.input.fileinputformat.input.dir.recursive": "true",
            "mapred.input.dir": path + '/*.meta'}
    sc = pyspark.SparkContext.getOrCreate()
    logger.info("loading metadata")
    files = sc.newAPIHadoopRDD(inputFormatClass, keyFormatClass, valueFormatClass, conf=conf)
    logger.info("parsing metadata")
    files = files.map(lambda x: parser.parse_line_meta(id_record=x[0], line=x[1]))

    return rdd_to_dataframe(files)


def to_list(x):
    l = list(x)
    l = [a for a in l if not pd.isnull(a)]
    return l


def rdd_to_dataframe(rdd):
    logger.info("collecting metadata")
    # collecting everything
    meta = rdd.collect()

    # turn into a dataframe
    df = pd.DataFrame.from_dict(meta)
    columns = df.columns

    # grouping by 'id_sample'
    g = df.groupby('id_sample')

    logger.info("dataframe construction")
    result_df = pd.DataFrame()
    for col in tqdm(columns, total=len(columns)):
        if col != 'id_sample':
            result_df[col] = g[col].apply(to_list)

    return result_df



