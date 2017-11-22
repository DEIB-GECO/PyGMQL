from ..parsers import get_parsing_function
import pandas as pd
from tqdm import tqdm
import logging
# from concurrent.futures import ThreadPoolExecutor
import math
from ..DataStructures import reg_fixed_fileds
from multiprocessing import pool, cpu_count
from functools import partial
from ... import none, Some

# Loading strategy
loading_strategy = "single_core"


def load_regions(collected_result):
    # get the number of regions
    n_regions = collected_result.getNumberOfRegions()
    # get the string delimiters
    regions_delimiter = collected_result.REGIONS_DELIMITER()
    values_delimiter = collected_result.VALUES_DELIMITER()
    # get how the strings are structured
    names, types = get_schema(collected_result)
    result = []

    from ... import is_progress_enabled
    if loading_strategy == 'single_core':
        # get the full string
        regions_string = collected_result.getRegionsAsString(none())
        if regions_string:
            # convert to list of strings
            regions_string = regions_string.split(regions_delimiter)
            iterator = map(lambda x: string_to_dictionary(x, values_delimiter, names, types),
                           tqdm(regions_string, disable=not is_progress_enabled()))
            result.extend(iterator)
    elif loading_strategy == 'multi_core':
        # number of divisions
        divisions = 10
        chunk_size = math.ceil(n_regions / divisions)
        p = pool.Pool(min(4, cpu_count()))

        std_partial = partial(string_to_dictionary, values_delimiter=values_delimiter,
                              names=names, types=types)

        for _ in tqdm(range(divisions), disable=not is_progress_enabled()):
            # get the full string
            regions_string = collected_result.getRegionsAsString(Some(chunk_size))
            if regions_string:
                # convert to list of strings
                regions_string = regions_string.split(regions_delimiter)

                iterator = p.map(std_partial, regions_string)
                result.extend(iterator)

        p.close()
    else:
        raise ValueError("Unknown loading mode ({})".format(loading_strategy))

    columns = reg_fixed_fileds + names
    if len(result) > 0:
        df = pd.DataFrame.from_dict(result)
        df = df.set_index(keys="id_sample", drop=True)
        df = df[columns]  # for ordering the columns
    else:
        df = pd.DataFrame(columns=columns)
        df.index.name = "id_sample"
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
        element = elements[5 + i]
        if element != 'null':
            d[name] = fun(element)
        else:
            d[name] = None
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
    for meta in meta_Java:
        id = int(meta[0])
        name = meta[1]
        value = meta[2]
        meta_list.append({'id_sample' : id, name: value})

    if len(meta_list) > 0:
        df = pd.DataFrame.from_dict(meta_list)
        columns = df.columns

        # grouping by 'id_sample'
        g = df.groupby('id_sample')

        result_df = pd.DataFrame()
        from ... import __disable_progress
        for col in tqdm(columns, total=len(columns), disable=__disable_progress):
            if col != 'id_sample':
                result_df[col] = g[col].apply(to_list)
    else:
        result_df = pd.DataFrame()
        result_df.index.name = "id_sample"
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
