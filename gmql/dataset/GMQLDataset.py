from .. import get_python_manager, get_gateway
from .loaders import MetaLoaderFile, RegLoaderFile
from .DataStructures.RegField import RegField
from .DataStructures.MetaField import MetaField
import shutil
import os

reg_fixed_fileds = ['chr', 'start', 'stop', 'strand']

class GMQLDataset:
    """
    The main abstraction of the library.
    A GMQLDataset represents a genomic dataset and it is divided in region data and
    meta data. The function that can be applied to a GMQLDataset affect one of these
    two features or both.
    """

    def __init__(self, parser=None, index=None, regs=None, meta=None):
        """
        Initiates a GMQLDataset
        :param parser: the parser for the dataset
        :param index: the index to be used to retrieve the IRVariable on the Scala side
        """
        self.parser = parser
        self.index = index
        self.regs = regs
        self.meta = meta

        self.pmg = get_python_manager()
        self.opmng = self.pmg.getOperatorManager()


    def show_info(self):
        print("GMQLDataset")
        print("\tParser:\t{}".format(self.parser.get_parser_name()))
        print("\tIndex:\t{}".format(self.index))

    def load_from_path(self, path, meta_load=False):
        """
        Load a GMQL Dataset
        :param path: path of the dataset
        :return: a new GMQLDataset
        """
        index = self.pmg.read_dataset(path, self.parser.get_parser_name())
        meta = None
        if meta_load:
            # load directly the metadata for exploration
            meta = MetaLoaderFile.load_meta_from_path(path)

        return GMQLDataset(parser=self.parser, index=index, meta=meta)

    def get_meta_attributes(self):
        """
        :return: the set of attributes of the metadata dataset
        """
        pass

    def get_reg_attributes(self):
        pass

    def MetaField(self, name):
        return MetaField(name=name)

    def RegField(self, name):
        return RegField(name=name, index=self.index)

    def meta_select(self, predicate):
        """
        Select the rows of the metadata dataset based on a logical predicate
        :param predicate: logical predicate on the values of the rows
        :return: a new GMQLDataset
        """
        meta_condition = predicate.getMetaCondition()

        new_index = self.opmng.meta_select(self.index, meta_condition)
        return GMQLDataset(index=new_index, parser=self.parser)

    def meta_project(self, attr_list):
        """
        Project the metadata based on a list of attribute names
        :param attr_list: list of the columns to select
        :param new_attr_dict: an optional dictionary of the form {'new_attribute_1': function1, ...}
                                in which every function computes the new column based on the values of the others
        :return: a new GMQLDataset
        """
        metaJavaList = get_gateway().jvm.java.util.ArrayList()
        for a in attr_list:
            metaJavaList.append(a)
        new_index = self.opmng.meta_project(self.index, metaJavaList)
        return GMQLDataset(index=new_index, parser=self.parser)

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
        reg_condition = predicate.getRegionCondition()

        new_index = self.opmng.reg_select(self.index, reg_condition)

        return GMQLDataset(parser=self.parser, index=new_index)

    def reg_project(self, field_list, new_field_dict=None):
        """
        Project the region data based on a list of field names
        :param field_list: list of the fields to select
        :param new_field_dict: an optional dictionary of the form {'new_field_1': function1, ...}
                                in which every function computes the new field based on the values 
                                of the others
        :return: a new GMQLDataset
        """
        regsJavaList = get_gateway().jvm.java.util.ArrayList()
        for f in field_list:
            if f not in reg_fixed_fileds:
                regsJavaList.append(f)
        if new_field_dict is not None:
            regExtJavaList = get_gateway().jvm.java.util.ArrayList()
            for k in new_field_dict.keys():
                name = k
                reNode = new_field_dict[k].getRegionExtension()
                reg_extension = self.pmg.getNewExpressionBuilder(self.index).createRegionExtension(name, reNode)
                regExtJavaList.append(reg_extension)
            new_index = self.opmng.reg_project_extend(self.index, regsJavaList, regExtJavaList)
        else:
            new_index = self.opmng.reg_project(self.index, regsJavaList)
        return GMQLDataset(index=new_index, parser=self.parser)

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

    def cover(self, type, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        COVER is a GMQL operator that takes as input a dataset (of usually, 
        but not necessarily, multiple samples) and returns another dataset 
        (with a single sample, if no groupby option is specified) by “collapsing”
        the input samples and their regions according to certain rules 
        specified by the COVER parameters. The attributes of the output regions 
        are only the region coordinates, plus in case, when aggregate functions 
        are specified, new attributes with aggregate values over attribute values 
        of the contributing input regions; output metadata are the union of the 
        input ones, plus the metadata attributes JaccardIntersect and 
        JaccardResult, representing global Jaccard Indexes for the considered 
        dataset, computed as the correspondent region Jaccard Indexes but on 
        the whole sample regions.

        :param type: the kind of cover variant you want
        :param minAcc: minimum accumulation value, i.e. the minimum number
         of overlapping regions to be considered during COVER execution
        :param maxAcc: maximum accumulation value, i.e. the maximum number
         of overlapping regions to be considered during COVER execution
        :param groupBy: optional list of metadata attributes 
        :param new_reg_fields: dictionary of the type
            {'new_region_attribute' : AGGREGATE_FUNCTION('field'), ...}
        :return: a new GMQLDataset
        """
        coverFlag = self.opmng.getCoverTypes(type)
        minAccParam = self.opmng.getCoverParam(str(minAcc))
        maxAccParam = self.opmng.getCoverParam(str(maxAcc))
        groupByJavaList = get_gateway().jvm.java.util.ArrayList()
        if groupBy:
            # translate to java list
            for g in groupBy:
                groupByJavaList.append(g)
        aggregatesJavaList = get_gateway().jvm.java.util.ArrayList()
        if new_reg_fields:
            expBuild = self.pmg.getNewExpressionBuilder(self.index)
            for k in new_reg_fields.keys():
                new_name = k
                op = new_reg_fields[k]
                op_name = op.get_aggregate_name()
                op_argument = op.get_argument()
                regsToReg = expBuild.getRegionsToRegion(op_name, new_name, op_argument)
                aggregatesJavaList.append(regsToReg)
        new_index = self.opmng.cover(self.index, coverFlag, minAccParam, maxAccParam,
                                     groupByJavaList, aggregatesJavaList)
        return GMQLDataset(index=new_index, parser=self.parser)

    def normal_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        return self.cover("normal", minAcc, maxAcc, groupBy, new_reg_fields)

    def flat_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        return self.cover("flat", minAcc, maxAcc, groupBy, new_reg_fields)

    def summit_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        return self.cover("summit", minAcc, maxAcc, groupBy, new_reg_fields)

    def histogram_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        return self.cover("histogram", minAcc, maxAcc, groupBy, new_reg_fields)

    def sample(self, fraction):
        """
        :param n: number of different samples to be sampled
        :return: a new GMQLDataset
        """
        pass

    def materialize(self, output_path):

        # check that the folder does not exists
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)

        self.pmg.materialize(self.index, output_path)

        # taking in memory the data structure
        real_path = output_path + '/exp/'
        # metadata
        meta = MetaLoaderFile.load_meta_from_path(real_path)
        # region data
        regs = RegLoaderFile.load_reg_from_path(real_path)
        return GMQLDataset(parser=self.parser, index=self.index, regs=regs, meta=meta)

