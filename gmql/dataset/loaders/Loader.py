from ... import get_python_manager
from ..parsers.Parser import Parser
from . import MetaLoaderFile, RegLoaderFile
from ..GDataframe import GDataframe
from .. import GMQLDataset
import os

def preprocess_path(path):
    for d in os.listdir(path):
        if d == 'exp':
            new_path = path + "/exp/"
            return new_path
    return path


def load_from_path(path, parser=None, meta_load=False, reg_load=False, all_load=False):
    """ Loads the data from a path into a GMQLDataset
    
    :param path: local path of the dataset
    :param parser: the parser to be used for reading the data
    :param meta_load: if set to True, the metadata are loaded directly into memory
    :param reg_load: if set to True, the region data are loaded directly into memory
    :param all_load: if set to True, both region and meta data are loaded in memory and an 
                     instance of GDataframe is returned
    :return: A new GMQLDataset or a GDataframe
    """
    pmg = get_python_manager()

    path = preprocess_path(path)
    index = None
    if parser:
        if type(parser) is str:
            index = pmg.read_dataset(path, parser)
        elif isinstance(parser, Parser):
            index = pmg.read_dataset(path, parser.get_gmql_parser())
        else:
            raise ValueError("parser must be a string or a Parser")
    elif all_load is False:
        index = pmg.read_dataset(path)

    if all_load:
        reg_load = True
        meta_load = True
    meta = None
    if meta_load:
        # load directly the metadata for exploration
        meta = MetaLoaderFile.load_meta_from_path(path)
    regs = None
    if reg_load:
        if isinstance(parser, Parser):
            # region data
            regs = RegLoaderFile.load_reg_from_path(path, parser)
        else:
            regs = RegLoaderFile.load_reg_from_path(path)
    if (regs is not None) and (meta is not None):
        return GDataframe(regs=regs, meta=meta)
    else:
        return GMQLDataset.GMQLDataset(index=index, parser=parser, regs=regs, meta=meta)

