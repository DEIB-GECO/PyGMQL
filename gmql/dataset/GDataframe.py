from .storers import FrameToGMQL
import pandas as pd
from .loaders import Loader
from .DataStructures import reg_fixed_fileds, \
    strand_aliases, stop_aliases, start_aliases, chr_aliases
import numpy as np

chr_types = [object, str]
start_types = [int, np.int, np.int8, np.int16, np.int32, np.int64]
stop_types = start_types
strand_types = [object, str]

class GDataframe:
    """ Class holding the result of a materialization of a GMQLDataset.
    It is composed by two data structures:
    
        - A table with the *region* data
        - A table with the *metadata* corresponding to the regions
    """
    def __init__(self, regs, meta):
        if not isinstance(regs, pd.DataFrame):
            raise TypeError("regs: expected pandas Dataframe, got {}".format(type(regs)))
        if not isinstance(meta, pd.DataFrame):
            raise TypeError("meta: expected pandas Dataframe, got {}".format(type(meta)))
        self.regs = regs
        self.meta = meta

    def to_genomic_space(self):
        """ Translates the GDataframe to the Genomic Space data structure
        """
        raise NotImplementedError("Not yet implemented")

    def to_dataset_files(self, local_path=None, remote_path=None):
        """ Save the GDataframe to a local or remote location

        :param local_path: a local path to the folder in which the data must be saved
        :param remote_path: a remote dataset name that wants to be used for these data
        :return: None
        """
        return FrameToGMQL.to_dataset_files(self, path_local=local_path, path_remote=remote_path)

    def to_GMQLDataset(self, local_path=None, remote_path=None):
        """ Converts the GDataframe in a GMQLDataset for later local or remote computation

        :return: a GMQLDataset
        """
        self.to_dataset_files(local_path, remote_path)
        if local_path is not None:
            return Loader.load_from_path(local_path=local_path)
        elif remote_path is not None:
            raise NotImplementedError()
        else:
            raise ValueError("You must specify at least one of local_path and remote_path")


def from_pandas(regs, meta=None, chr_name=None, start_name=None, stop_name=None, strand_name=None):
    """ Creates a GDataframe from a pandas dataframe of region and a pandas dataframe of metadata

    :param regs: a pandas Dataframe of regions that is coherent with the GMQL data model
    :param meta: (optional) a pandas Dataframe of metadata that is coherent with the regions
    :param chr_name: (optional) which column of :attr:`~.regs` is the chromosome
    :param start_name: (optional) which column of :attr:`~.regs` is the start
    :param stop_name: (optional) which column of :attr:`~.regs` is the stop
    :param strand_name: (optional) which column of :attr:`~.regs` is the strand
    :return: a GDataframe
    """
    regs = check_regs(regs, chr_name, start_name, stop_name, strand_name)
    regs = to_gmql_regions(regs)
    if meta is not None:
        if not check_meta(meta, regs):
            raise ValueError("Error. Meta dataframe is not GMQL standard")
    else:
        meta = empty_meta(regs)
    return GDataframe(regs, meta)


def check_regs(region_df, chr_name=None, start_name=None, stop_name=None, strand_name=None):
    """ Modifies a region dataframe to be coherent with the GMQL data model

    :param region_df: a pandas Dataframe of regions that is coherent with the GMQL data model
    :param chr_name: (optional) which column of :attr:`~.region_df` is the chromosome
    :param start_name: (optional) which column of :attr:`~.region_df` is the start
    :param stop_name: (optional) which column of :attr:`~.region_df` is the stop
    :param strand_name: (optional) which column of :attr:`~.region_df` is the strand
    :return: a modified pandas Dataframe
    """
    region_df = search_column(region_df,  chr_aliases, chr_types, 'chr', chr_name)
    region_df = search_column(region_df, start_aliases, start_types, 'start', start_name)
    region_df = search_column(region_df, stop_aliases, stop_types, 'stop', stop_name)
    region_df = search_column(region_df, strand_aliases, strand_types, 'strand', strand_name)
    return region_df


def search_column(region_df, names, types, subs, name=None):
    columns = region_df.columns.map(str.lower)
    names = list(map(str.lower, names))

    if name is not None:
        if check_type(region_df[name], types):
            region_df = region_df.rename({name: subs})
        else:
            raise TypeError("Column {} is not of type {}.".format(name, types))

    isok = False
    for e in columns:
        if e in names and check_type(region_df[e], types):
            region_df = region_df.rename({e: subs})
            isok = True
            break
    if (not isok) and (subs != 'strand'):
        raise ValueError("{} column was not found".format(subs))
    return region_df


def check_type(column, types):
    return column.dtype in types


def check_meta(meta_df, regs_df):
    return True


def empty_meta(regs):
    index = regs.index.unique()
    return pd.DataFrame(index=index)


def to_gmql_regions(regs):
    cols = ['chr', 'start', 'stop']
    if 'strand' in regs.columns:
        cols.append('strand')
    cols.extend([c for c in regs.columns if c not in cols])
    return regs[cols]
