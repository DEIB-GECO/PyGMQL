from .. import GDataframe
from tqdm import tqdm
import logging
from . import *
import os
import shutil
from ..parsers import TAB, COORDS_DEFAULT

logger = logging.getLogger("PyGML logger")


def to_dataset_files(gframe, path_local=None, path_remote=None):
    if not isinstance(gframe, GDataframe.GDataframe):
        raise TypeError("Expected GDataframe, got {}".format(type(gframe)))

    if not check_gmql_coherent(gframe):
        raise ValueError("The GDataframe is not coherent with the GMQL standard")

    if path_local is not None:
        to_local(gframe, path_local)

    if path_remote is not None:
        to_remote(gframe, path_remote)


def to_local(gframe, path_local):
    regs = gframe.regs
    meta = gframe.meta

    # creating the directory
    if os.path.isdir(path_local):
        shutil.rmtree(path_local)
    os.makedirs(path_local)

    logger.info("Saving data at {}".format(path_local))
    all_file_names = regs.index.unique()
    from ... import __disable_progress
    for file in tqdm(all_file_names, disable=__disable_progress):
        final_file_name = os.path.join(path_local, str(file))
        part_regs = regs.loc[[file]]
        part_regs.to_csv(path_or_buf=final_file_name, sep="\t", header=False, index=False)

        parts_meta = meta.loc[[file]]
        meta_to_file(parts_meta, final_file_name + ".meta")

    # save the schema
    dataset_name = os.path.basename(os.path.normpath(path_local))
    save_schema(regs, dataset_name, TAB, os.path.join(path_local, "schema.schema"), COORDS_DEFAULT)


def meta_to_file(meta_df, filename):
    f = open(filename, "w")
    # the dataframe is only one line
    d = meta_df.iloc[0].to_dict()

    for k in d.keys():
        line_template = "{}\t{}\n"
        data = d[k]
        for el in data:
            line = line_template.format(k, el)
            f.write(line)
    f.close()


def save_schema(regs_df, dataset_name, dataset_type, schema_file_name, coordinate_system):
    column_names = regs_df.columns.tolist()
    types = regs_df.dtypes
    result = schema_header
    result += schema_collection_template.format(dataset_name)
    result += schema_dataset_type_template.format(dataset_type, coordinate_system)
    for c in column_names:
        t = pyType_to_scalaType[types[c].name].upper()
        result += schema_field_template.format(t, c)
    result += schema_dataset_end
    result += schema_collection_end

    with open(schema_file_name, "w") as f:
        f.write(result)


def to_remote(gframe, path_remote):
    pass


def check_gmql_coherent(gframe):
    # index coherency
    regs_idxs = set(gframe.regs.index.unique())
    meta_idxs = set(gframe.meta.index.unique())
    if regs_idxs != meta_idxs:
        return False
    return True


