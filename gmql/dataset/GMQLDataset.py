import pandas as pd
from .loaders import MetaLoader, RegLoader
import gmql.operators as op


class GMQLDataset:
    """
    The main abstraction of the library.
    A GMQLDataset represents a genomic dataset and it is divided in:
        - region data: a classic RDD
        - metadata: a pandas dataframe
    A region sample is composed by
    """

    def __init__(self, reg_dataset=None, meta_dataset=None, parser=None):
        self.reg_dataset = reg_dataset
        self.meta_dataset = meta_dataset
        self.parser = parser

    def load_from_path(self, path):
        """
        Load a GMQL Dataset
        :param path: path of the dataset
        :return: a new GMQLDataset
        """

        # load metadata
        self.meta_dataset = MetaLoader.load_meta_from_path(path, self.parser)

        # load region data
        self.reg_dataset = RegLoader.load_reg_from_path(path, self.parser)

        return GMQLDataset(reg_dataset=self.reg_dataset, meta_dataset=self.meta_dataset, parser=self.parser)

    def meta_select(self, predicate):
        meta = op.meta_select(self.meta_dataset, predicate)

        # TODO : filter the regions

        return GMQLDataset(reg_dataset=self.reg_dataset, meta_dataset=meta)
