from glob import glob
from tqdm import tqdm
import logging

from ..parsers.GenericMetaParser import GenericMetaParser
from . import generateKey
import os

logger = logging.getLogger('gmql_logger')


def load_meta_from_path(path):
    meta_files = glob(pathname=path + '/*.meta')
    parsed = []
    parser = GenericMetaParser()
    logger.info("Loading meta data from path {}".format(path))
    for f in tqdm(meta_files, total=len(meta_files)):
        abs_path = os.path.abspath(f)
        key = generateKey(abs_path)
        lines = open(abs_path).readlines()
        # parsing
        parsed.extend(list(map(lambda row: parser.parse_line_reg(key, row), lines)))  # [(id, (attr_name, value)),...]
    return to_pandas(parsed)

def to_pandas(meta_list):
    # TODO
    pass