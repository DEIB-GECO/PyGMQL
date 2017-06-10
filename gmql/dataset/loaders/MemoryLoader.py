from ..parsers import get_parsing_function
import pandas as pd
from tqdm import tqdm
import logging
from concurrent.futures import ThreadPoolExecutor
import math
from ..DataStructures import reg_fixed_fileds

# global logger
logger = logging.getLogger("PyGML logger")

# number of divisions
divisions = 100


def load_regions(collected_result):
    # get the number of regions
    n_regions = collected_result.getNumberOfRegions()
    chunk_size = math.ceil(n_regions / divisions)
    # get the string delimiters
    regions_delimiter = collected_result.REGIONS_DELIMITER()
    values_delimiter = collected_result.VALUES_DELIMITER()
    # get how the strings are structured
    names, types = get_schema(collected_result)
    result = []
    executor = ThreadPoolExecutor()
    for i in tqdm(range(divisions)):
        # get the full string
        regions_string = collected_result.getRegionsAsString(chunk_size)
        if regions_string:
            # convert to list of strings
            regions_string = regions_string.split(regions_delimiter)
            iterator = executor.map(lambda row:
                                    string_to_dictionary(row,
                                                         values_delimiter,
                                                         names,
                                                         types), regions_string)
            result.extend(list(iterator))
    df = pd.DataFrame.from_dict(result)
    columns = reg_fixed_fileds + names
    df = df.set_index(keys="id_sample", drop=True)
    df = df[columns]    # for ordering the columns
    return df


def string_to_dictionary(region, values_delimiter, names, types):
    elements = region.split(values_delimiter)
    # print(elements)
    d = dict()
    d['id_sample'] = int(elements[0])
    d['chr'] = elements[1]
    d['start'] = int(elements[2])
    d['stop'] = int(elements[3])
    d['strand'] = elements[4]

    for i in range(0, len(names)):
        name = names[i]
        fun = types[i]
        d[name] = fun(elements[5 + i])

    return d


def to_dictionary(region, names, types):
    d = dict()
    d['id_sample'] = int(region[0])
    d['chr'] = region[1]
    d['start'] = int(region[2])
    d['stop'] = int(region[3])
    d['strand'] = region[4]

    for i in range(0, len(names)):
        name = names[i]
        fun = types[i]
        d[name] = fun(region[5 + i])

    return d


def load_metadata(collected_result):
    meta_Java = collected_result.getMetadata()
    meta_list = []
    logger.info("Loading metadata")
    for meta in meta_Java:
        id = int(meta[0])
        name = meta[1]
        value = meta[2]
        meta_list.append({'id_sample' : id, name: value})

    df = pd.DataFrame.from_dict(meta_list)
    columns = df.columns

    # grouping by 'id_sample'
    g = df.groupby('id_sample')

    logger.info("Building metadata dataframe")
    result_df = pd.DataFrame()
    for col in tqdm(columns, total=len(columns)):
        if col != 'id_sample':
            result_df[col] = g[col].apply(to_list)
    return result_df


def to_list(x):
    l = list(x)
    l = [a for a in l if not pd.isnull(a)]
    return l


def get_schema(collected_result):
    schema_Java = collected_result.getSchema()
    # schema_Java = [[name, type], [name, type], ...]
    names = []
    types = []
    for field in schema_Java:
        name = field[0]
        type_string = field[1]
        fun = get_parsing_function(type_string)

        names.append(name)
        types.append(fun)
    return names, types
