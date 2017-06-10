from ... import get_python_manager
from ..parsers.Parser import Parser
from . import MetaLoaderFile, RegLoaderFile
from .. import GDataframe
from .. import GMQLDataset
import os


def preprocess_path(path):
    for d in os.listdir(path):
        if d == 'exp':
            new_path = os.path.join(path, "exp")
            return new_path
    return path


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
        return GMQLDataset.GMQLDataset(index=index, parser=parser, regs=regs, meta=meta)


def load_from_remote(remote_name=None, parser=None, meta_load=False,
                     reg_load=False, all_load=False):
    """ Loads the data from a remote repository.

    :param remote_name: The name of the dataset in the remote repository
    :return A new GMQLDataset or a GDataframe
    """
    # TODO: download from remote to 'path' and then call 'load_from_path'
    pass

