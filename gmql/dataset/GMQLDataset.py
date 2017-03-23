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

    def get_meta_attributes(self):
        return self.meta_dataset.columns.tolist()

    def meta_select(self, predicate):
        meta = op.meta_select(self.meta_dataset, predicate)

        # TODO : filter the regions

        return GMQLDataset(reg_dataset=self.reg_dataset, meta_dataset=meta)

    def meta_project(self, attr_list, new_attr_dict=None):
        """
        Project the metadata based on a list of attribute names
        :param attr_list: list of the columns to select
        :param new_attr_dict: an optional dictionary of the form {'new_attribute_1': function1, ...}
                                in which every function computes the new column based on the values of the others
        :return: a new GMQLDataset
        """
        meta = op.meta_project(self.meta_dataset, attr_list, new_attr_dict)
        return GMQLDataset(reg_dataset=self.reg_dataset, meta_dataset=meta)

    def add_meta(self, attr_name, value):
        """
        Adds to the metadata dataset a new column in which the same value is assigned to each sample
        :param attr_name: name of the attribute to add
        :param value: value to add to each sample
        :return: a new GMQLDataset
        """
        if type(value) is not list:
            value = [value]
        new_attr_dict = {attr_name : lambda row : value}
        return self.meta_project(self.get_meta_attributes(), new_attr_dict)




