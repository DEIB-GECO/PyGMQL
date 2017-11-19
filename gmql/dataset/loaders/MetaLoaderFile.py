from glob import glob
from tqdm import tqdm
import logging

from ..parsers.MetadataParser import GenericMetaParser
from . import generateNameKey
import os
import pandas as pd

logger = logging.getLogger("PyGML logger")


def load_meta_from_path(path):
    meta_files = glob(pathname=path + '/*.meta')
    parsed = []
    parser = GenericMetaParser()
    logger.info("Loading meta data from path {}".format(path))
    from ... import __disable_progress
    for f in tqdm(meta_files, total=len(meta_files), disable=__disable_progress):
        abs_path = os.path.abspath(f)
        abs_path_no_meta = abs_path[:-5]
        key = generateNameKey(abs_path_no_meta)
        ps = parser.parse_metadata(abs_path)                # [(attr_name, value), ...]
        ps = list(map(lambda x: (key, (x[0], x[1])), ps))   # [(id, (attr_name, value)),...]
        # parsing
        parsed.extend(ps)
    return to_pandas(parsed)


def to_pandas(meta_list):
    # turn to dictionary
    if len(meta_list) > 0:
        meta_list = list(map(to_dictionary, meta_list))     # [{'id_sample': id, attr_name: value},...]
        df = pd.DataFrame.from_dict(meta_list)
        columns = df.columns

        # grouping by 'id_sample'
        g = df.groupby('id_sample')

        logger.info("dataframe construction")
        result_df = pd.DataFrame()
        from ... import __disable_progress
        for col in tqdm(columns, total=len(columns), disable=__disable_progress):
            if col != 'id_sample':
                result_df[col] = g[col].apply(to_list)
    else:
        result_df = pd.DataFrame()
        result_df.index.name = "id_sample"
    return result_df


def to_dictionary(tuple):
    return {"id_sample": tuple[0], tuple[1][0]: tuple[1][1]}


def to_list(x):
    l = list(x)
    l = [a for a in l if not pd.isnull(a)]
    return l