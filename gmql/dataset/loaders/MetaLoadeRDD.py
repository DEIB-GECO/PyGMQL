import pyspark
import logging
from . import inputFormatClass, keyFormatClass, valueFormatClass, conf
from .Materializations import meta_rdd_to_pandas, pandas_to_rdd
"""
    Loading functons for Metadata
"""

logger = logging.getLogger('gmql_logger')


def load_meta_from_path(path, parser):
    conf_meta = conf.copy()
    conf_meta["mapred.input.dir"] = path + '/*.meta'
    sc = pyspark.SparkContext.getOrCreate()
    
    logger.info("loading metadata")
    meta = sc.newAPIHadoopRDD(inputFormatClass, keyFormatClass, valueFormatClass, conf=conf_meta)
    logger.info("parsing metadata")
    meta = meta.map(lambda x: parser.parse_line_meta(id_record=x[0], line=x[1]))
    # RDD[(id, (attr_name, value))]

    # convert to  for local processing
    meta_df = meta_rdd_to_pandas(meta)
    # go back to rdd
    meta = pandas_to_rdd(meta_df)   # RDD[(id, MetaRecord)]
    return meta, meta_df.columns.tolist()
