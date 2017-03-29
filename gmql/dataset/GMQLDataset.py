from .loaders import MetaLoader, RegLoader
import gmql.operators as op
import pandas as pd


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
        """
        :return: the set of attributes of the metadata dataset
        """
        return self.meta_dataset.columns.tolist()

    def get_reg_attributes(self):
        return self.parser.get_attributes()

    def meta_select(self, predicate):
        """
        Select the rows of the metadata dataset based on a logical predicate
        :param predicate: logical predicate on the values of the rows
        :return: a new GMQLDataset
        """
        meta = op.meta_select(self.meta_dataset, predicate)

        # get all the regions corresponding to the selected samples
        id_samples = meta.index.tolist()
        reg = self.reg_dataset.filter(lambda sample: sample['id_sample'] in id_samples)

        return GMQLDataset(reg_dataset=reg, meta_dataset=meta)

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
        new_attr_dict = {attr_name : lambda row : value}
        return self.meta_project(self.get_meta_attributes(), new_attr_dict)

    """
    Region operations
    """
    def get_reg_sample(self, n):
        """
        Materialize n samples of regions from the RDD
        :param n: number of samples to materialize
        :return: a pandas dataframe
        """
        regs = self.reg_dataset.take(n)
        regs = list(map(from_tuple_to_dict, regs))
        return pd.DataFrame.from_dict(regs)

    def reg_select(self, predicate):
        """
        Select only the regions in the dataset that satisfy the predicate
        :param predicate: logical predicate on the values of the regions
        :return: a new GMQLDataset
        """
        regs = op.reg_select(self.reg_dataset, predicate)
        return GMQLDataset(reg_dataset=regs, meta_dataset=self.meta_dataset)

    def reg_project(self, field_list, new_field_dict=None):
        """
        Project the region data based on a list of field names
        :param field_list: list of the fields to select
        :param new_field_dict: an optional dictionary of the form {'new_field_1': function1, ...}
                                in which every function computes the new field based on the values 
                                of the others
        :return: a new GMQLDataset
        """
        regs = op.reg_project(self.reg_dataset, field_list, new_field_dict)
        return GMQLDataset(reg_dataset=regs, meta_dataset=self.meta_dataset)

    """
    Mixed operations
    """

    def extend(self, new_attr_dict):
        """
        Extend the metadata based on aggregations on regions
        :param new_attr_dict: dictionary of the form {'new_attribute_1': function1, ...}
                                in which every function takes a dictionary of the type
                                {
                                    'field1' : [value1, value2, ...],
                                    'field2' : [value1, ...],
                                    ...
                                    'fieldN' : [value1, ...]
                                }
        :return: a new GMQLDataset
        """
        


def from_tuple_to_dict(tuple):
    tuple[1]['id_sample'] = tuple[0]
    return tuple[1]