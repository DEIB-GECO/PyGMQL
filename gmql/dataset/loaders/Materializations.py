import pandas as pd
from tqdm import tqdm
import logging
import pyspark
from ..DataStructures.MetaRecord import MetaRecord

logger = logging.getLogger('gmql_logger')


def meta_rdd_to_pandas(meta):
    # make an rdd of dictionaries
    meta = meta.map(lambda t: {'id_sample': t[0], t[1][0]: t[1][1]})

    # collect everything
    meta = meta.collect()   # [{'id_sample': id, attr_name: value}, ...]

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


def to_list(x):
    l = list(x)
    l = [a for a in l if not pd.isnull(a)]
    return l


def pandas_to_rdd(df):
    sc = pyspark.SparkContext.getOrCreate()
    d = df.to_dict('index')
    items = d.items()     # [(id, {attr_name: value, ...})]
    items = list(
        map(lambda x: (x[0], create_meta_record(x[1])), items)
    )
    rdd = sc.parallelize(items)   # RDD[(id, MetaRecord)]
    return rdd


def create_meta_record(dictionary):
    meta_record = MetaRecord()
    meta_record.from_dict(dictionary)
    return meta_record
