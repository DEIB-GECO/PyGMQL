from .. import get_python_manager, get_gateway, none, Some, get_remote_manager
from .loaders import MetaLoaderFile, RegLoaderFile
from .DataStructures.RegField import RegField
from .DataStructures.MetaField import MetaField
import shutil
import os
from .DataStructures import reg_fixed_fileds
from .import GDataframe
from .loaders import MemoryLoader, Loader
from ..FileManagment.TempFileManager import get_unique_identifier, get_new_dataset_tmp_folder


class GMQLDataset:
    """
    The main abstraction of the library.
    A GMQLDataset represents a genomic dataset in the GMQL standard and it is divided
    in region data and meta data. The function that can be applied to a GMQLDataset
    affect one of these two features or both.
    """

    def __init__(self, parser=None, index=None, regs=None, meta=None, location="local"):
        self.parser = parser
        self.index = index
        self.regs = regs
        self.meta = meta
        self.location = location

        self.pmg = get_python_manager()
        self.opmng = self.pmg.getOperatorManager()

        # get the schema of the dataset
        schemaJava = self.pmg.getVariableSchemaNames(self.index)
        self.schema = []
        for field in schemaJava:
            self.schema.append(field)
        self.schema.extend(reg_fixed_fileds)

        # setting the schema as properties of the dataset
        for field in self.schema:
            self.__setattr__(field, self.RegField(field))
        # add also left and right
        self.left = self.RegField("left")
        self.right = self.RegField("right")

    def __getitem__(self, item):
        if isinstance(item, str):
            return self.MetaField(item)
        elif isinstance(item, MetaField):
            return self.meta_select(predicate=item)
        else:
            raise ValueError("Input must be a string or a MetaField. {} was found".format(type(item)))

    def __show_info(self):
        print("GMQLDataset")
        print("\tParser:\t{}".format(self.parser.get_parser_name()))
        print("\tIndex:\t{}".format(self.index))

    def get_reg_attributes(self):
        """
        Returns the region fields of the dataset
        
        :return: a list of field names
        """
        return self.schema

    def MetaField(self, name):
        """
        Creates an instance of a metadata field of the dataset. It can be used in building expressions
        or conditions for projection or selection.
        Notice that this function is equivalent to call::
            
            dataset[name]
        
        :param name: the name of the metadata that is considered
        :return: a MetaField instance
        """
        return MetaField(name=name, index=self.index)

    def RegField(self, name):
        """
        Creates an instance of a region field of the dataset. It can be used in building expressions
        or conditions for projection or selection.
        Notice that this function is equivalent to::
        
            dataset.name
        
        :param name: the name of the region field that is considered
        :return: a RegField instance
        """
        return RegField(name=name, index=self.index)

    def meta_select(self, predicate=None, semiJoinDataset=None, semiJoinMeta=None):
        """
        The META_SELECT operation creates a new dataset from an existing one 
        by extracting a subset of samples from the input dataset; each sample 
        in the output dataset has the same region attributes and metadata 
        as in the input dataset.
        The selection can be based on:
        
            * *Metadata predicates*: selection based on the existence and values of certain 
              metadata attributes in each sample. In predicates, attribute-value conditions 
              can be composed using logical predicates && (and), || (or) and ~ (not)
            * *SemiJoin clauses*: selection based on the existence of certain metadata :attr:`~.semiJoinMeta`
              attributes and the matching of their values with those associated with at 
              least one sample in an external dataset :attr:`~.semiJoinDataset`
        
        :param predicate: logical predicate on the values of the rows
        :param semiJoinDataset: an other GMQLDataset 
        :param semiJoinMeta: a list of metadata
        :return: a new GMQLDataset

        
        Example 1::
        
            output_dataset = dataset.meta_select(dataset['patient_age'] < 70)
            # This statement can be written also as
            output_dataset = dataset[ dataset['patient_age'] < 70 ]

        Example 2::

            output_dataset = dataset.meta_select( (dataset['tissue_status'] == 'tumoral') &&
                                                (tumor_tag == 'gbm') || (tumor_tag == 'brca'))
            # This statement can be written also as
            output_dataset = dataset[ (dataset['tissue_status'] == 'tumoral') &&
                                    (tumor_tag == 'gbm') || (tumor_tag == 'brca') ]

        Example 3::

            JUN_POLR2A_TF = HG19_ENCODE_NARROW.meta_select( JUN_POLR2A_TF['antibody_target'] == 'JUN',
                                                            semiJoinDataset=POLR2A_TF, semiJoinMeta=['cell'])

        """

        other_idx = None
        metaJoinCondition = None
        meta_condition = None

        if isinstance(predicate, MetaField):
            meta_condition = predicate.getMetaCondition()
        if (semiJoinDataset is not None) and  \
                (semiJoinMeta is not None):
            other_idx = semiJoinDataset.index
            metaJoinByJavaList = get_gateway().jvm.java.util.ArrayList()
            for meta in semiJoinMeta:
                metaJoinByJavaList.append(meta)
            metaJoinCondition = self.opmng.getMetaJoinCondition(metaJoinByJavaList)

        if other_idx is None:
            other_idx = -1

        if (meta_condition is None) and \
                (other_idx >= 0 and metaJoinCondition is not None): # case of only semiJoin
            new_index = self.opmng.only_semi_select(self.index, other_idx, metaJoinCondition)
        elif meta_condition is not None:
            if (other_idx >= 0) and (metaJoinCondition is not None): # case of meta + semiJoin
                new_index = self.opmng.meta_select(self.index, other_idx, meta_condition, metaJoinCondition)
            elif (other_idx < 0) and (metaJoinCondition is None): # case only meta
                new_index = self.opmng.meta_select(self.index, other_idx, meta_condition)
            else:
                raise ValueError("semiJoinDataset <=> semiJoinMeta")
        else:
            raise ValueError("semiJoinDataset <=> semiJoinMeta")

        return GMQLDataset(index=new_index, location=self.location)

    def reg_select(self, predicate=None, semiJoinDataset=None, semiJoinMeta=None):
        """
        Select only the regions in the dataset that satisfy the predicate

        :param predicate: logical predicate on the values of the regions
        :param semiJoinDataset: an other GMQLDataset 
        :param semiJoinMeta: a list of metadata
        :return: a new GMQLDataset

        An example of usage::

            new_dataset = dataset.reg_select(dataset.chr == 'chr1' || dataset.pValue < 0.9)
        """
        reg_condition = None
        other_idx = None
        metaJoinCondition = None

        if predicate is not None:
            reg_condition = predicate.getRegionCondition()
        if (semiJoinDataset is not None) and \
                (semiJoinMeta is not None):
            other_idx = semiJoinDataset.index
            metaJoinByJavaList = get_gateway().jvm.java.util.ArrayList()
            for meta in semiJoinMeta:
                metaJoinByJavaList.append(meta)
            metaJoinCondition = self.opmng.getMetaJoinCondition(metaJoinByJavaList)

        if other_idx is None:
            other_idx = -1

        if (reg_condition is None) and \
                (other_idx >= 0 and metaJoinCondition is not None):  # case of only semiJoin
            new_index = self.opmng.only_semi_select(self.index, other_idx, metaJoinCondition)
        elif reg_condition is not None:
            if (other_idx >= 0) and (metaJoinCondition is not None):  # case of regs + semiJoin
                new_index = self.opmng.reg_select(self.index, other_idx, reg_condition, metaJoinCondition)
            elif (other_idx < 0) and (metaJoinCondition is None):  # case only regs
                new_index = self.opmng.reg_select(self.index, other_idx, reg_condition)
            else:
                raise ValueError("semiJoinDataset <=> semiJoinMeta")
        else:
            raise ValueError("semiJoinDataset <=> semiJoinMeta")

        return GMQLDataset(index=new_index, location=self.location)

    def meta_project(self, attr_list=None, all_but=None, new_attr_dict=None):
        """
        Project the metadata based on a list of attribute names
        
        :param attr_list: list of the metadata fields to select
        :param new_attr_dict: an optional dictionary of the form {'new_field_1': function1,
               'new_field_2': function2, ...} in which every function computes
               the new field based on the values of the others
        :return: a new GMQLDataset
        """
        if (attr_list is not None) and (all_but is not None):
            raise ValueError("You can specifiy only one of attr_list or all_but. Not both")
        all_but_value = False
        projected_meta = None
        if (attr_list is None) and (all_but is None):
            projected_meta = none()
        elif isinstance(attr_list, list):
            projected_meta = Some(attr_list)
        elif isinstance(all_but, list):
            projected_meta = Some(all_but)
            all_but_value = True
        else:
            raise ValueError("attr_list must be a list")

        meta_ext = None
        if new_attr_dict is None:
            meta_ext = none()
        elif not isinstance(new_attr_dict, dict):
            raise ValueError("new_attr_list must be a dictionary")
        else:
            if len(new_attr_dict) != 1:
                raise ValueError("The dictionary must have only one mapping")
            meta_ext_list = []
            for k in new_attr_dict.keys():
                name = k
                meNode = new_attr_dict[k]
                if not isinstance(meNode, MetaField):
                    raise ValueError("the values of the dictionary must be MetaField")
                meNode = meNode.getMetaExpression()
                meta_extension = self.pmg.getNewExpressionBuilder(self.index) \
                    .createMetaExtension(name, meNode)
                meta_ext_list.append(meta_extension)
            meta_ext = Some(meta_ext_list[0])

        new_index = self.opmng.project(self.index,
                                       projected_meta,
                                       meta_ext, all_but_value, none(), none(), none())

        return GMQLDataset(index=new_index, location=self.location)

    def reg_project(self, field_list=None, all_but=None, new_field_dict=None):
        """
        Project the region data based on a list of field names

        :param field_list: list of the fields to select
        :param all_but: keep only the region fields different from the ones
               specified
        :param new_field_dict: an optional dictionary of the form {'new_field_1':
               function1, 'new_field_2': function2, ...} in which every function
               computes the new field based on the values of the others
        :return: a new GMQLDataset

        An example of usage::

            new_dataset = dataset.reg_project(['pValue', 'name'],
                                            {'new_field': dataset.pValue / 2})
        """
        projected_regs = None
        if field_list is None:
            projected_regs = none()
        elif isinstance(field_list, list):
            projected_regs = Some(field_list)
        else:
            raise ValueError("field_list must be a list")

        all_but_f = None
        if all_but is None:
            all_but_f = none()
        elif isinstance(all_but, list):
            all_but_f = Some(all_but)
        else:
            raise ValueError("all_but list must be a list")

        regs_ext = None
        if new_field_dict is None:
            regs_ext = none()
        elif not isinstance(new_field_dict, dict):
            raise ValueError("new_filed_dict must be a dictionary")
        else:
            regs_ext_list = []
            for k in new_field_dict.keys():
                name = k
                reNode = new_field_dict[k]
                if not isinstance(reNode, RegField):
                    raise ValueError("the values of the dictionary must be RegField")
                reNode = reNode.getRegionExpression()
                reg_extension = self.pmg.getNewExpressionBuilder(self.index)\
                    .createRegionExtension(name, reNode)
                regs_ext_list.append(reg_extension)
            regs_ext = Some(regs_ext_list)

        new_index = self.opmng.project(self.index, none(), none(),
                                       projected_regs, all_but_f, regs_ext)
        return GMQLDataset(index=new_index, location=self.location)

    def extend(self, new_attr_dict):
        """
        For each sample in an input dataset, the EXTEND operator builds new metadata attributes,
        assigns their values as the result of arithmetic and/or aggregate functions calculated on 
        sample region attributes, and adds them to the existing metadata attribute-value pairs of 
        the sample. Sample number and their genomic regions, with their attributes and values, 
        remain unchanged in the output dataset.  
        
        :param new_attr_dict: a dictionary of the type {'new_metadata' : AGGREGATE_FUNCTION('field'), ...}
        :return: new GMQLDataset
        
        An example of usage::
        
            import gmql as gl
            
            # ....previous operations on the dataset
            
            new_dataset = dataset.extend({'regionCount' : gl.COUNT(),
                                          'minPValue' : gl.MIN('pValue')})
        """
        expBuild = self.pmg.getNewExpressionBuilder(self.index)
        aggregatesJavaList = get_gateway().jvm.java.util.ArrayList()
        for k in new_attr_dict.keys():
            new_name = k
            op = new_attr_dict[k]
            op_name = op.get_aggregate_name()
            op_argument = op.get_argument()
            regsToMeta = expBuild.getRegionsToMeta(op_name, new_name, op_argument)
            aggregatesJavaList.append(regsToMeta)

        new_index = self.opmng.extend(aggregatesJavaList)
        return GMQLDataset(index=new_index, location=self.location)

    def cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None, type="normal"):
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

        :param type: the kind of cover variant you want ['normal', 'flat', 'summit', 'histogram']
        :param minAcc: minimum accumulation value, i.e. the minimum number
         of overlapping regions to be considered during COVER execution. It can be any positive
         number or the strings {'ALL', 'ANY'}.
        :param maxAcc: maximum accumulation value, i.e. the maximum number
         of overlapping regions to be considered during COVER execution. It can be any positive
         number or the strings {'ALL', 'ANY'}.
        :param groupBy: optional list of metadata attributes 
        :param new_reg_fields: dictionary of the type
            {'new_region_attribute' : AGGREGATE_FUNCTION('field'), ...}
        :return: a new GMQLDataset
        
        An example of usage::
        
            cell_tf = narrow_peak.cover("normal", minAcc=1, maxAcc="Any", 
                                            groupBy=['cell', 'antibody_target'])    
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
        return GMQLDataset(index=new_index, location=self.location)

    def normal_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        The normal cover operation as described in :meth:`~.cover`.
        Equivalent to calling::
        
            dataset.cover("normal", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, type="normal")

    def flat_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        Variant of the function :meth:`~.cover` that returns the union of all the regions 
        which contribute to the COVER. More precisely, it returns the contiguous regions 
        that start from the first end and stop at the last end of the regions which would 
        contribute to each region of a COVER.
        
        Equivalent to calling::
        
            cover("flat", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, type="flat")

    def summit_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        Variant of the function :meth:`~.cover` that returns only those portions of the COVER
        result where the maximum number of regions overlap (this is done by returning only
        regions that start from a position after which the number of overlaps does not 
        increase, and stop at a position where either the number of overlapping regions decreases 
        or violates the maximum accumulation index).
        
        Equivalent to calling::
        
            cover("summit", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, type="summit")

    def histogram_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        Variant of the function :meth:`~.cover` that returns all regions contributing to 
        the COVER divided in different (contiguous) parts according to their accumulation 
        index value (one part for each different accumulation value), which is assigned to 
        the AccIndex region attribute.
        
        Equivalent to calling::
        
            cover("histogram", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, type="histogram")

    def join(self, experiment, genometric_predicate, output="LEFT", joinBy=None,
             refName=None, expName=None):
        """
        The JOIN operator takes in input two datasets, respectively known as anchor
        (the first/left one) and experiment (the second/right one) and returns a
        dataset of samples consisting of regions extracted from the operands 
        according to the specified condition (known as genometric predicate). 
        The number of generated output samples is the Cartesian product of the number 
        of samples in the anchor and in the experiment dataset (if no joinby close 
        if specified). The attributes (and their values) of the regions in the output 
        dataset are the union of the region attributes (with their values) in the input 
        datasets; homonymous attributes are disambiguated by prefixing their name with 
        their dataset name. The output metadata are the union of the input metadata, 
        with their attribute names prefixed with their input dataset name.
        
        :param experiment: an other GMQLDataset
        :param genometric_predicate: a list of Genometric atomic conditions
        :param output: one of four different values that declare which region is given in output 
                       for each input pair of anchor and experiment regions satisfying the genometric predicate:
                       
                       * 'LEFT': outputs the anchor regions from the anchor dataset that satisfy the genometric predicate
                       * 'RIGHT': outputs the anchor regions from the experiment dataset that satisfy the genometric predicate
                       * 'INT': outputs the overlapping part (intersection) of the anchor and experiment regions that satisfy
                         the genometric predicate; if the intersection is empty, no output is produced
                       * 'CONTIG': outputs the concatenation between the anchor and experiment regions that satisfy the 
                         genometric predicate, i.e. the output region is defined as having left (right) coordinates 
                         equal to the minimum (maximum) of the corresponding coordinate values in the anchor and 
                         experiment regions satisfying the genometric predicate


        :param joinBy: list of metadata attributes
        :param refName: name that you want to assign to the reference dataset
        :param expName: name that you want to assign to the experiment dataset
        :return: a new GMQLDataset
        
        An example of usage::
            
            import gmql as gl
            
            # anchor_dataset and experiment_dataset are created
            
            result_dataset = anchor_dataset.join(experiment=experiment_dataset, 
                                                    genometric_predicate=[gl.MD(1), gl.DGE(120000)], 
                                                    output="right")
        """
        atomicConditionsJavaList = get_gateway().jvm.java.util.ArrayList()
        for a in genometric_predicate:
            atomicConditionsJavaList.append(a.get_gen_condition())
        regionJoinCondition = self.opmng.getRegionJoinCondition(atomicConditionsJavaList)
        metaJoinByJavaList = get_gateway().jvm.java.util.ArrayList()
        if joinBy:
            for m in joinBy:
                metaJoinByJavaList.append(m)
        metaJoinCondition = self.opmng.getMetaJoinCondition(metaJoinByJavaList)
        regionBuilder = self.opmng.getRegionBuilderJoin(output)

        if refName is None:
            refName = ""
        if expName is None:
            expName = ""

        new_index = self.opmng.join(self.index, experiment.index,
                                    metaJoinCondition, regionJoinCondition, regionBuilder,
                                    refName, expName)

        return GMQLDataset(index=new_index, location=self.location)

    def map(self, experiment, new_reg_fields=None, joinBy=None, refName=None, expName=None):
        """
        MAP is a non-symmetric operator over two datasets, respectively called
        reference and experiment. The operation computes, for each sample in 
        the experiment dataset, aggregates over the values of the experiment 
        regions that intersect with a region in a reference sample, for each 
        region of each sample in the reference dataset; we say that experiment 
        regions are mapped to the reference regions.
        The number of generated output samples is the Cartesian product of the samples 
        in the two input datasets; each output sample has the same regions as the related 
        input reference sample, with their attributes and values, plus the attributes 
        computed as aggregates over experiment region values. Output sample metadata 
        are the union of the related input sample metadata, whose attribute names are 
        prefixed with their input dataset name. 
        For each reference sample, the MAP operation produces a matrix like structure,
        called genomic space, where each experiment sample is associated with a row, 
        each reference region with a column, and each matrix row is a vector of numbers
        - the aggregates computed during MAP execution. When the features of the reference 
        regions are unknown, the MAP helps in extracting the most interesting regions 
        out of many candidates. 
        
        :param experiment: a GMQLDataset
        :param new_reg_fields: an optional dictionary of the form 
               {'new_field_1': AGGREGATE_FUNCTION(field), ...}
                                
        :param joinBy: optional list of metadata
        :param refName: name that you want to assign to the reference dataset
        :param expName: name that you want to assign to the experiment dataset
        :return: a new GMQLDataset
        """
        aggregatesJavaList = get_gateway().jvm.java.util.ArrayList()
        if new_reg_fields:
            expBuild = self.pmg.getNewExpressionBuilder(experiment.index)
            for k in new_reg_fields.keys():
                new_name = k
                op = new_reg_fields[k]
                op_name = op.get_aggregate_name()
                op_argument = op.get_argument()
                regsToReg = expBuild.getRegionsToRegion(op_name, new_name, op_argument)
                aggregatesJavaList.append(regsToReg)
        metaJoinByJavaList = get_gateway().jvm.java.util.ArrayList()
        if joinBy:
            for m in joinBy:
                metaJoinByJavaList.append(m)
        metaJoinCondition = self.opmng.getMetaJoinCondition(metaJoinByJavaList)

        if refName is None:
            refName = ""
        if expName is None:
            expName = ""

        new_index = self.opmng.map(self.index, experiment.index, metaJoinCondition,
                                   aggregatesJavaList, refName, expName)
        return GMQLDataset(index=new_index, location=self.location)

    def order(self, meta=None, meta_ascending=None, meta_top=None, meta_k=None,
              regs=None, regs_ascending=None, region_top=None, region_k=None):
        """
        The ORDER operator is used to order either samples, sample regions, or both,
        in a dataset according to a set of metadata and/or region attributes, and/or 
        region coordinates. The number of samples and their regions in the output dataset 
        is as in the input dataset, as well as their metadata and region attributes 
        and values, but a new ordering metadata and/or region attribute is added with 
        the sample or region ordering value, respectively. 
        
        :param meta: list of metadata attributes
        :param meta_ascending: list of boolean values (True = ascending, False = descending)
        :param meta_top: "top" or "topq" or None
        :param meta_k: a number specifying how many results to be retained
        :param regs: list of region attributes
        :param regs_ascending: list of boolean values (True = ascending, False = descending)
        :param region_top: "top" or "topq" or None
        :param region_k: a number specifying how many results to be retained
        :return: a new GMQLDataset
        """
        assert meta or regs, "There must be at least an ordering on the " \
                             "metadata or on the region data"
        assert (meta_top and meta_k) or (not meta_top and not meta_k), \
            "meta_top and meta_k must be together"
        assert (region_top and region_k) or (not region_top and not region_k), \
            "region_top and region_k must be together"
        assert (meta_ascending and len(meta_ascending) == len(meta)) or not meta_ascending, \
            "meta_ascending and meta must have the same length"
        assert (regs_ascending and len(regs_ascending) == len(regs)) or not regs_ascending, \
            "regs_ascending and regs must have the same length"

        if not meta_top:
            meta_top = ""
            meta_k = ""

        if not region_top:
            region_top = ""
            region_k = ""

        if not meta:
            meta = []
            meta_ascending = []

        if not regs:
            regs = []
            regs_ascending = []

        new_index = self.opmng.order(self.index, meta, meta_ascending, meta_top, str(meta_k),
                                     regs, regs_ascending, region_top, str(region_k))
        return GMQLDataset(index=new_index, location=self.location)

    def difference(self, other, joinBy=None, exact=None):
        """
        DIFFERENCE is a binary, non-symmetric operator that produces one sample
        in the result for each sample of the first operand, by keeping the same
        metadata of the first operand sample and only those regions (with their
        schema and values) of the first operand sample which do not intersect with
        any region in the second operand sample (also known as negative regions)
        
        :param other: GMQLDataset
        :param joinBy: list of metadata attributes
        :param exact: boolean
        :return: a new GMQLDataset
        """
        metaJoinByJavaList = get_gateway().jvm.java.util.ArrayList()
        if joinBy:
            for m in joinBy:
                metaJoinByJavaList.append(m)
        if not exact:
            exact = False
        metaJoinCondition = self.opmng.getMetaJoinCondition(metaJoinByJavaList)
        new_index = self.opmng.difference(self.index, other.index, metaJoinCondition, exact)
        return GMQLDataset(index=new_index, location=self.location)

    def union(self, other, left_name="", right_name=""):
        """
        The UNION operation is used to integrate homogeneous or heterogeneous samples of two
        datasets within a single dataset; for each sample of either one of the input datasets, a
        sample is created in the result as follows:
        
            * its metadata are the same as in the original sample;
            * its schema is the schema of the first (left) input dataset; new 
              identifiers are assigned to each output sample;
            * its regions are the same (in coordinates and attribute values) as in the original
              sample. Region attributes which are missing in an input dataset sample
              (w.r.t. the merged schema) are set to null.
                
        :param other: a GMQLDataset
        :param left_name: name that you want to assign to the left dataset
        :param right_name: name that you want to assign to the right dataset
        :return: a new GMQLDataset
        """
        new_index = self.opmng.union(self.index, other.index, left_name, right_name)
        return GMQLDataset(index=new_index, location=self.location)

    def merge(self, groupBy=None):
        """
        The MERGE operator builds a new dataset consisting of a single sample having
        
            * as regions all the regions of all the input samples, with the 
              same attributes and values
            * as metadata the union of all the metadata attribute-values 
              of the input samples.
                
        A groupby clause can be specified on metadata: the samples are then
        partitioned in groups, each with a distinct value of the grouping metadata 
        attributes, and the MERGE operation is applied to each group separately, 
        yielding to one sample in the result dataset for each group.
        Samples without the grouping metadata attributes are disregarded
        
        :param groupBy: list of metadata attributes
        :return: a new GMQLDataset
        """
        groupByJavaList = get_gateway().jvm.java.util.ArrayList()
        if groupBy:
            # translate to java list
            for g in groupBy:
                groupByJavaList.append(g)
        new_index = self.opmng.merge(self.index, groupByJavaList)
        return GMQLDataset(index=new_index, location=self.location)
    """
        Materialization utilities
    """
    def materialize(self, output_path=None, output_name=None,  all_load=True):
        """ Starts the execution of the operations for the GMQLDataset. PyGMQL implements lazy execution
        and no operation is performed until the materialization of the results is requestd.
        This operation can happen both locally or remotely.

        * Local mode: if the GMQLDataset is local (based on local data) the user can specify the

        :param output_path: (Optional) If specified, the user can say where to locally save the results
                            of the computations.
        :param output_name: (Optional) Can be used only if the dataset is remote. It represents the name that
                            the user wants to give to the resulting dataset on the server
        :param all_load: (Optional) It affects the computation only when the dataset is remote. It specifies if
                         the downloaded result dataset should be directly converted to a GDataframe (True) or to a
                         GMQLDataset (False) for future local queries.
        :return: A GDataframe or a GMQLDataset
        """

        if self.location == 'remote':
            return self._materialize_remote(output_name, output_path, all_load)
        elif self.location == 'local':
            if output_name is not None:
                raise ValueError("This dataset is local. You cannot specify a result name.")
            return self._materialize_local(output_path)
        else:
            raise ValueError("GMQLDataset location unknown: {}".format(self.location))

    def _materialize_remote(self, output_name=None, download_path=None, all_load=True):
        if not isinstance(output_name, str):
            output_name = get_unique_identifier()
        self.pmg.materialize(self.index, output_name)
        remote_manager = get_remote_manager()
        if (download_path is None) and all_load:
            download_path = get_new_dataset_tmp_folder()
        result = remote_manager.execute_remote_all(output_path=download_path)
        if len(result) == 1: #TODO: change this!!!
            path = result[0]
            return Loader.load_from_path(local_path=path, all_load=all_load)

    def _materialize_local(self, output_path=None):
        regs = None
        meta = None
        if output_path is not None:
            # check that the folder does not exists
            if os.path.isdir(output_path):
                shutil.rmtree(output_path)

            self.pmg.materialize(self.index, output_path)
            self.pmg.execute()
            # taking in memory the data structure
            real_path = os.path.join(output_path, 'exp')
            # metadata
            meta = MetaLoaderFile.load_meta_from_path(real_path)
            # region data
            regs = RegLoaderFile.load_reg_from_path(real_path)

        else:
            # We load the structure directly from the memory
            collected = self.pmg.collect(self.index)
            regs = MemoryLoader.load_regions(collected)
            meta = MemoryLoader.load_metadata(collected)

        result = GDataframe.GDataframe(regs=regs, meta=meta)
        return result

    def take(self, n=5):
        """ Returns a small set of regions and metadata from a query. It is supposed to
        be used for debugging purposes or for data exploration.

        :param n: how many samples to retrieve
        :return: a GDataframe
        """
        if n <= 0:
            raise ValueError("n must be a positive number. {} was given".format(n))

        collected = self.pmg.take(self.index, n)
        regs = MemoryLoader.load_regions(collected)
        meta = MemoryLoader.load_metadata(collected)
        result = GDataframe.GDataframe(regs, meta)
        return result

    def _get_serialized_dag(self):
        serialized_dag = self.pmg.serializeVariable(self.index)
        return serialized_dag
