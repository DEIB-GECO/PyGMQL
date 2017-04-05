from .loaders import MetaLoader, RegLoader, MetaSampleCreation, RegSampleCreation
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

    def __init__(self, reg_dataset=None, reg_attributes=None,
                 meta_dataset=None, meta_attributes=None,
                 parser=None):
        self._reg_dataset = reg_dataset
        self._reg_attributes = reg_attributes
        self._meta_dataset = meta_dataset
        self._meta_attributes = meta_attributes
        self.parser = parser

    def load_from_path(self, path):
        """
        Load a GMQL Dataset
        :param path: path of the dataset
        :return: a new GMQLDataset
        """

        # load metadata: RDD[(id, MetaRecord)]
        self._meta_dataset, self._meta_attributes = MetaLoader.load_meta_from_path(path, self.parser)

        # load region data: RDD[(id, RegRecord)]
        self._reg_dataset, self._reg_attributes = RegLoader.load_reg_from_path(path, self.parser)

        return GMQLDataset(reg_dataset=self._reg_dataset, meta_dataset=self._meta_dataset, parser=self.parser)

    def get_meta_attributes(self):
        """
        :return: the set of attributes of the metadata dataset
        """
        return self._meta_attributes

    def get_reg_attributes(self):
        return self._reg_attributes

    def meta_select(self, predicate):
        """
        Select the rows of the metadata dataset based on a logical predicate
        :param predicate: logical predicate on the values of the rows
        :return: a new GMQLDataset
        """
        meta = op.meta_select(self._meta_dataset, predicate)

        # get all the regions corresponding to the selected samples
        regs = self._reg_dataset.join(meta.map(lambda x: (x[0], 0)))  # RDD[(id, (RegRecord, 0))]
        regs = regs.map(lambda x: (x[0], x[1][0]))  # RDD[(id, RegRecord)]

        return GMQLDataset(reg_dataset=regs, meta_dataset=meta)

    def meta_project(self, attr_list, new_attr_dict=None):
        """
        Project the metadata based on a list of attribute names
        :param attr_list: list of the columns to select
        :param new_attr_dict: an optional dictionary of the form {'new_attribute_1': function1, ...}
                                in which every function computes the new column based on the values of the others
        :return: a new GMQLDataset
        """
        meta = op.meta_project(self._meta_dataset, attr_list, new_attr_dict)
        return GMQLDataset(reg_dataset=self._reg_dataset, meta_dataset=meta)

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
        regs = self._reg_dataset.take(n)
        regs = list(map(from_tuple_to_dict, regs))
        return pd.DataFrame.from_dict(regs)

    def reg_select(self, predicate):
        """
        Select only the regions in the dataset that satisfy the predicate
        :param predicate: logical predicate on the values of the regions
        :return: a new GMQLDataset
        """
        regs = op.reg_select(self._reg_dataset, predicate)
        return GMQLDataset(reg_dataset=regs, meta_dataset=self._meta_dataset)

    def reg_project(self, field_list, new_field_dict=None):
        """
        Project the region data based on a list of field names
        :param field_list: list of the fields to select
        :param new_field_dict: an optional dictionary of the form {'new_field_1': function1, ...}
                                in which every function computes the new field based on the values 
                                of the others
        :return: a new GMQLDataset
        """
        regs = op.reg_project(self._reg_dataset, field_list, new_field_dict)
        return GMQLDataset(reg_dataset=regs, meta_dataset=self._meta_dataset)

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
        regs = op.extend(self._reg_dataset, new_attr_dict)

        # collecting
        regs = regs.collect()

        # transformation into metadata pandas dataframe
        df = pd.DataFrame.from_dict(regs)
        df = df.set_index('id_sample')

        new_meta = pd.concat(objs=[self._meta_dataset, df], axis=1, join='inner')
        return GMQLDataset(reg_dataset=self._reg_dataset, meta_dataset=new_meta)


def from_tuple_to_dict(tuple):
    tuple[1]['id_sample'] = tuple[0]
    return tuple[1]