from .storers import FrameToGMQL
import pandas as pd


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

    def to_GMQLDataset(self):
        """ Converts the GDataframe in a GMQLDataset for later local or remote computation

        :return: a GMQLDataset
        """
        return None