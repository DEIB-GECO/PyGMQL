from . import inputFormatClass, keyFormatClass, valueFormatClass, conf
import logging
from glob import glob

import pyspark
"""
    Loading functions for region data
"""
# global logger
logger = logging.getLogger('gmql_logger')


def load_reg_from_path(path, parser):

    # we need to take only the files of the regions, so only the files that does NOT end with '.meta'
    all_files = set(glob(pathname=path + '/*'))
    meta_files = set(glob(pathname=path + '/*.meta'))

    only_region_files = all_files - meta_files
    only_region_files = ', '.join(only_region_files)

    conf_meta = conf.copy()
    conf_meta["mapred.input.dir"] = only_region_files
    sc = pyspark.SparkContext.getOrCreate()

    logger.info("loading region data")
    files = sc.newAPIHadoopRDD(inputFormatClass, keyFormatClass, valueFormatClass, conf=conf_meta)
    logger.info("parsing region data")
    files = files.map(lambda x: parser.parse_line_reg(id_record=x[0], line=x[1]))

    return files
