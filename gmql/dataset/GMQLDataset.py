class GMQLDataset:
    """
    The main abstraction of the library.
    A GMQLDataset represents a genomic dataset and it is divided in:
        - region data: a classic RDD
        - metadata: a pandas dataframe
    A region sample is composed by
    """

    def __init__(self):
        pass

    def load_from_path(self, path):
        """
        Load a GMQL Dataset
        :param path: path of the dataset
        :return: a new GMQLDataset
        """
        pass

    def get_meta_attributes(self):
        """
        :return: the set of attributes of the metadata dataset
        """
        pass

    def get_reg_attributes(self):
        pass

    def meta_select(self, predicate):
        """
        Select the rows of the metadata dataset based on a logical predicate
        :param predicate: logical predicate on the values of the rows
        :return: a new GMQLDataset
        """
        pass

    def meta_project(self, attr_list, new_attr_dict=None):
        """
        Project the metadata based on a list of attribute names
        :param attr_list: list of the columns to select
        :param new_attr_dict: an optional dictionary of the form {'new_attribute_1': function1, ...}
                                in which every function computes the new column based on the values of the others
        :return: a new GMQLDataset
        """
        pass

    def add_meta(self, attr_name, value):
        """
        Adds to the metadata dataset a new column in which the same value is assigned to each sample
        :param attr_name: name of the attribute to add
        :param value: value to add to each sample
        :return: a new GMQLDataset
        """
        pass

    """
    Region operations
    """
    def get_reg_sample(self, n):
        """
        Materialize n samples of regions from the RDD
        :param n: number of samples to materialize
        :return: a pandas dataframe
        """
        pass

    def reg_select(self, predicate):
        """
        Select only the regions in the dataset that satisfy the predicate
        :param predicate: logical predicate on the values of the regions
        :return: a new GMQLDataset
        """
        pass

    def reg_project(self, field_list, new_field_dict=None):
        """
        Project the region data based on a list of field names
        :param field_list: list of the fields to select
        :param new_field_dict: an optional dictionary of the form {'new_field_1': function1, ...}
                                in which every function computes the new field based on the values 
                                of the others
        :return: a new GMQLDataset
        """
        pass

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
        pass

    def sample(self, fraction):
        """
        :param n: number of different samples to be sampled
        :return: a new GMQLDataset
        """
        pass