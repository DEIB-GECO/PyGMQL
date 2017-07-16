from ... import get_python_manager, get_remote_manager
from ..parsers.Parser import Parser
from . import MetaLoaderFile, RegLoaderFile
from .. import GDataframe
from .. import GMQLDataset
import os
import glob


def get_file_paths(path):
    real_path = preprocess_path(path)
    files_paths = set(glob.glob(real_path + "/[!_]*"))
    schema_path = set(glob.glob(real_path+"/*.schema"))
    files_paths = files_paths - schema_path
    return list(files_paths), schema_path.pop()


def preprocess_path(path):
    for root, dirs, files in os.walk(path):
        if check_for_dataset(files):
            return root
    raise ValueError("The provided path does not contain any GMQL dataset")


def check_for_dataset(files):
    return len([x for x in files if x.endswith(".meta")]) > 0


def load_from_path(local_path=None, parser=None, meta_load=False,
                   reg_load=False, all_load=False):
    """ Loads the data from a local path into a GMQLDataset.
    The loading of the files is "lazy", which means that the files are loaded only when the
    user does a materialization (see :func:`~gmql.dataset.GMQLDataset.GMQLDataset.materialize` ).
    The user can force the materialization of the data (maybe for an initial data exploration on
    only the metadata) by setting the :attr:`~.reg_load` (load in memory the region data),
    :attr:`~.meta_load` (load in memory the metadata) or :attr:`~.all_load` (load both region and
    meta data in memory). If the user specifies this final parameter as True, a
    :class:`~gmql.dataset.GDataframe.GDataframe` is returned, otherwise a
    :class:`~gmql.dataset.GMQLDataset.GMQLDataset` is returned
    
    :param local_path: local path of the dataset
    :param parser: the parser to be used for reading the data
    :param meta_load: if set to True, the metadata are loaded directly into memory
    :param reg_load: if set to True, the region data are loaded directly into memory
    :param all_load: if set to True, both region and meta data are loaded in memory and an 
                     instance of GDataframe is returned
    :return: A new GMQLDataset or a GDataframe
    """
    pmg = get_python_manager()

    local_path = preprocess_path(local_path)
    index = None
    if parser:
        if type(parser) is str:
            index = pmg.read_dataset(local_path, parser)
        elif isinstance(parser, Parser):
            index = pmg.read_dataset(local_path, parser.get_gmql_parser())
        else:
            raise ValueError("parser must be a string or a Parser")
    elif all_load is False:
        index = pmg.read_dataset(local_path)

    if all_load:
        reg_load = True
        meta_load = True
    meta = None
    if meta_load:
        # load directly the metadata for exploration
        meta = MetaLoaderFile.load_meta_from_path(local_path)
    regs = None
    if reg_load:
        if isinstance(parser, Parser):
            # region data
            regs = RegLoaderFile.load_reg_from_path(local_path, parser)
        else:
            regs = RegLoaderFile.load_reg_from_path(local_path)
    if (regs is not None) and (meta is not None):
        return GDataframe.GDataframe(regs=regs, meta=meta)
    else:
        return GMQLDataset.GMQLDataset(index=index, parser=parser, regs=regs, meta=meta, location="local")


def load_from_remote(remote_name, owner=None):
    """ Loads the data from a remote repository.

    :param remote_name: The name of the dataset in the remote repository
    :param owner: (optional) The owner of the dataset. If nothing is provided, the current user
                  is used. For public datasets use 'public'.
    :return A new GMQLDataset or a GDataframe
    """
    pmg = get_python_manager()
    remote_manager = get_remote_manager()
    parser = remote_manager.get_dataset_schema(remote_name, owner)
    index = pmg.read_dataset(remote_name, parser.get_gmql_parser())
    return GMQLDataset.GMQLDataset(index=index, location="remote")

