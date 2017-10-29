from glob import glob
import tqdm
import os
import xml.etree.ElementTree as ET
from ..parsers.BedParser import BedParser
import logging
import pandas as pd
from . import generateNameKey
from ..DataStructures import reg_fixed_fileds, \
    chr_aliases, start_aliases, stop_aliases, strand_aliases
import numpy as np
from ..parsers.Parser import parse_strand


# global logger
logger = logging.getLogger("PyGML logger")


def load_reg_from_path(path, parser=None):
    if parser is None:
        # get the parser for the dataset
        parser = get_parser(path)
    # we need to take only the files of the regions, so only the files that does NOT end with '.meta'
    all_files = set(glob(pathname=path + '/*'))
    meta_files = set(glob(pathname=path + '/*.meta'))

    only_region_files = all_files - meta_files
    logger.info("Loading region data from path {}".format(path))

    n_files = len(only_region_files)
    dfs = []
    from ... import __disable_progress
    for file in tqdm.tqdm(only_region_files, total=n_files, disable=__disable_progress):
        if file.endswith("schema") or file.endswith("_SUCCESS") or \
           file.endswith(".xml"):
            continue
        abs_path = os.path.abspath(file)
        key = generateNameKey(abs_path)

        df = pd.read_csv(filepath_or_buffer=abs_path, na_values="null",
                         header=None,
                         names=parser.get_ordered_attributes(),
                         dtype=parser.get_name_type_dict(),
                         sep="\t",
                         converters={'strand': parse_strand})

        df.index = np.repeat(key, len(df))
        dfs.append(df)
    if len(dfs) > 0:
        result = pd.concat(objs=dfs, copy=False)
    else:
        result = pd.DataFrame(columns=parser.get_ordered_attributes())
        result.index.name = "id_sample"
    return result


def get_parser(path):
    schema_file = glob(pathname=path + '/*.schema')[0]
    tree = ET.parse(schema_file)
    gmqlSchema = tree.getroot().getchildren()[0]
    parser_name = gmqlSchema.get('type')
    field_nodes = gmqlSchema.getchildren()

    i = 0
    chrPos, startPos, stopPos, strandPos, otherPos = None, None, None, None, None
    otherPos = []
    for field in field_nodes:
        name = list(field.itertext())[0].lower()
        type = field.get('type').lower()

        if name in chr_aliases:
            chrPos = i
        elif name in start_aliases:
            startPos = i
        elif name in stop_aliases:
            stopPos = i
        elif name in strand_aliases:
            strandPos = i
        else: # other positions
            otherPos.append((i, name, type))
        i += 1

    return BedParser(parser_name=parser_name, delimiter='\t',
                     chrPos=chrPos, startPos=startPos, stopPos=stopPos,
                     strandPos=strandPos, otherPos=otherPos)


def to_pandas(reg_list):
    df = pd.DataFrame.from_dict(reg_list)
    df = df[reg_fixed_fileds + [c for c in df.columns if c not in reg_fixed_fileds]]
    return df


def to_dictionary(tuple):
    d = tuple[1]
    d['id_sample'] = tuple[0]
    return d


