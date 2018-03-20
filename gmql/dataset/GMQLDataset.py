from .. import get_python_manager, none, Some, \
    get_remote_manager, get_mode, _get_source_table
from .loaders import MetaLoaderFile, Materializations
from .DataStructures.RegField import RegField
from .DataStructures.MetaField import MetaField
from .DataStructures.Aggregates import *
from .DataStructures.GenometricPredicates import GenometricCondition
from .DataStructures import reg_fixed_fileds
from .import GDataframe
from .loaders import MemoryLoader, MetadataProfiler
from ..FileManagment.TempFileManager import get_unique_identifier, get_new_dataset_tmp_folder
from .loaders.Sources import PARSER, LOCAL, REMOTE
from .storers.parserToXML import parserToXML
import os


class GMQLDataset:
    """
    The main abstraction of the library.
    A GMQLDataset represents a genomic dataset in the GMQL standard and it is divided
    in region data and meta data. The function that can be applied to a GMQLDataset
    affect one of these two features or both.
    """

    def __init__(self, parser=None, index=None, location="local", path_or_name=None,
                 local_sources=None, remote_sources=None,
                 meta_profile=None):
        self.parser = parser
        self.__index = index
        self.location = location
        self.meta_profile = None
        if isinstance(meta_profile, MetadataProfiler.MetadataProfile):
            self.meta_profile = meta_profile
        elif meta_profile is not None:
            raise TypeError("meta_profile must be MetadataProfiler")
        self.path_or_name = path_or_name
        self.pmg = get_python_manager()
        self.opmng = self.pmg.getOperatorManager()

        # provenance
        if isinstance(local_sources, list):
            self._local_sources = local_sources
        else:
            self._local_sources = []
        if isinstance(remote_sources, list):
            self._remote_sources = remote_sources
        else:
            self._remote_sources = []

        # get the schema of the dataset
        schemaJava = self.pmg.getVariableSchemaNames(self.__index)
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

    def __getitem__(self, arg):
        if isinstance(arg, tuple) and len(arg) == 2:
            item, t = arg
            return self.MetaField(name=item, t=t)
        elif isinstance(arg, str):
            return self.MetaField(name=arg)
        elif isinstance(arg, MetaField):
            return self.meta_select(predicate=arg)
        else:
            raise ValueError("Input must be a string or a MetaField. {} was found".format(type(arg)))

    def __show_info(self):
        print("GMQLDataset")
        print("\tParser:\t{}".format(self.parser.get_parser_name()))
        print("\tIndex:\t{}".format(self.__index))

    def get_metadata(self):
        if self.path_or_name is None:
            raise ValueError("You cannot explore the metadata of an intermediate query."
                             "You can get metadata only after a load_from_local or load_from_remote")
        if self.location == 'local':
            return self.__get_metadata_local()
        elif self.location == 'remote':
            return self.__get_metadata_remote()

    def __get_metadata_local(self):
        meta = MetaLoaderFile.load_meta_from_path(self.path_or_name)
        return meta

    def __get_metadata_remote(self):
        pass

    def get_reg_attributes(self):
        """
        Returns the region fields of the dataset
        
        :return: a list of field names
        """
        return self.schema

    def MetaField(self, name, t=None):
        """
        Creates an instance of a metadata field of the dataset. It can be used in building expressions
        or conditions for projection or selection.
        Notice that this function is equivalent to call::
            
            dataset[name]
        
        :param name: the name of the metadata that is considered
        :param t: the type of the metadata attribute

        :return: a MetaField instance
        """
        return MetaField(name=name, index=self.__index, t=t)

    def RegField(self, name):
        """
        Creates an instance of a region field of the dataset. It can be used in building expressions
        or conditions for projection or selection.
        Notice that this function is equivalent to::
        
            dataset.name
        
        :param name: the name of the region field that is considered
        :return: a RegField instance
        """
        return RegField(name=name, index=self.__index)

    def select(self, meta_predicate=None, region_predicate=None,
               semiJoinDataset=None,  semiJoinMeta=None):

        semiJoinDataset_exists = False
        if isinstance(meta_predicate, MetaField):
            meta_condition = Some(meta_predicate.getMetaCondition())
        elif meta_predicate is None:
            meta_condition = none()
        else:
            raise TypeError("meta_predicate must be a MetaField or None."
                            " {} was provided".format(type(meta_predicate)))

        if isinstance(region_predicate, RegField):
            region_condition = Some(region_predicate.getRegionCondition())
        elif region_predicate is None:
            region_condition = none()
        else:
            raise TypeError("region_predicate must be a RegField or None."
                            " {} was provided".format(type(region_predicate)))

        if isinstance(semiJoinDataset, GMQLDataset):
            other_dataset = Some(semiJoinDataset.__index)
            semiJoinDataset_exists = True
        elif semiJoinDataset is None:
            other_dataset = none()
        else:
            raise TypeError("semiJoinDataset must be a GMQLDataset or None."
                            " {} was provided".format(type(semiJoinDataset)))

        if isinstance(semiJoinMeta, list) and \
                all([isinstance(x, str) for x in semiJoinMeta]):
            if semiJoinDataset_exists:
                semi_join = Some(self.opmng.getMetaJoinCondition(semiJoinMeta))
            else:
                raise ValueError("semiJoinDataset and semiJoinMeta must be present at the "
                                 "same time or totally absent")
        elif semiJoinMeta is None:
            semi_join = none()
        else:
            raise TypeError("semiJoinMeta must be a list of strings or None."
                            " {} was provided".format(type(semiJoinMeta)))

        new_index = self.opmng.select(self.__index, other_dataset,
                                      semi_join, meta_condition, region_condition)
        return GMQLDataset(index=new_index, location=self.location, local_sources=self._local_sources,
                           remote_sources=self._remote_sources, meta_profile=self.meta_profile)

    def meta_select(self, predicate=None, semiJoinDataset=None, semiJoinMeta=None):
        """
        The META_SELECT operation creates a new dataset from an existing one 
        by extracting a subset of samples from the input dataset; each sample 
        in the output dataset has the same region attributes and metadata 
        as in the input dataset.
        The selection can be based on:
        
            * *Metadata predicates*: selection based on the existence and values of certain 
              metadata attributes in each sample. In predicates, attribute-value conditions 
              can be composed using logical predicates & (and), | (or) and ~ (not)
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

            output_dataset = dataset.meta_select( (dataset['tissue_status'] == 'tumoral') &
                                                (tumor_tag != 'gbm') | (tumor_tag == 'brca'))
            # This statement can be written also as
            output_dataset = dataset[ (dataset['tissue_status'] == 'tumoral') &
                                    (tumor_tag != 'gbm') | (tumor_tag == 'brca') ]

        Example 3::

            JUN_POLR2A_TF = HG19_ENCODE_NARROW.meta_select( JUN_POLR2A_TF['antibody_target'] == 'JUN',
                                                            semiJoinDataset=POLR2A_TF, semiJoinMeta=['cell'])

        The meta selection predicate can use all the classical equalities and disequalities
        {>, <, >=, <=, ==, !=} and predicates can be connected by the classical logical symbols
        {& (AND), | (OR), ~ (NOT)} plus the *isin* function.

        """

        return self.select(meta_predicate=predicate, semiJoinDataset=semiJoinDataset, semiJoinMeta=semiJoinMeta)

    def reg_select(self, predicate=None, semiJoinDataset=None, semiJoinMeta=None):
        """
        Select only the regions in the dataset that satisfy the predicate

        :param predicate: logical predicate on the values of the regions
        :param semiJoinDataset: an other GMQLDataset 
        :param semiJoinMeta: a list of metadata
        :return: a new GMQLDataset

        An example of usage::

            new_dataset = dataset.reg_select((dataset.chr == 'chr1') | (dataset.pValue < 0.9))

        You can also use Metadata attributes in selection::

            new_dataset = dataset.reg_select(dataset.score > dataset['size'])

        This statement selects all the regions whose field score is strictly higher than the sample
        metadata attribute size.

        The region selection predicate can use all the classical equalities and disequalities
        {>, <, >=, <=, ==, !=} and predicates can be connected by the classical logical symbols
        {& (AND), | (OR), ~ (NOT)} plus the *isin* function.

        In order to be sure about the correctness of the expression, please use parenthesis to delimit
        the various predicates.
        """
        return self.select(region_predicate=predicate, semiJoinMeta=semiJoinMeta, semiJoinDataset=semiJoinDataset)

    def project(self, projected_meta=None, new_attr_dict=None, all_but_meta=None,
                projected_regs=None, new_field_dict=None, all_but_regs=None):
        projected_meta_exists = False
        if isinstance(projected_meta, list) and \
                all([isinstance(x, str) for x in projected_meta]):
            projected_meta = Some(projected_meta)
            projected_meta_exists = True
        elif projected_meta is None:
            projected_meta = none()
        else:
            raise TypeError("projected_meta must be a list of strings or None."
                            " {} was provided".format(type(projected_meta)))

        if isinstance(new_attr_dict, dict):
            meta_ext = []
            expBuild = self.pmg.getNewExpressionBuilder(self.__index)
            for k in new_attr_dict.keys():
                item = new_attr_dict[k]
                if isinstance(k, str):
                    if isinstance(item, MetaField):
                        me = expBuild.createMetaExtension(k, item.getMetaExpression())
                    elif isinstance(item, int):
                        me = expBuild.createMetaExtension(k, expBuild.getMEType("int", str(item)))
                    elif isinstance(item, str):
                        me = expBuild.createMetaExtension(k, expBuild.getMEType("string", item))
                    elif isinstance(item, float):
                        me = expBuild.createMetaExtension(k, expBuild.getMEType("float", str(item)))
                    else:
                        raise TypeError("Type {} of item of new_attr_dict is not valid".format(type(item)))
                    meta_ext.append(me)
                else:
                    raise TypeError("The key of new_attr_dict must be a string. "
                                    "{} was provided".format(type(k)))
            meta_ext = Some(meta_ext)
        elif new_attr_dict is None:
            meta_ext = none()
        else:
            raise TypeError("new_attr_dict must be a dictionary."
                            " {} was provided".format(type(new_attr_dict)))

        if isinstance(all_but_meta, list) and \
            all([isinstance(x, str) for x in all_but_meta]):
            if not projected_meta_exists:
                all_but_meta = Some(all_but_meta)
                all_but_value = True
            else:
                raise ValueError("all_but_meta and projected_meta are mutually exclusive")
        elif all_but_meta is None:
            all_but_meta = none()
            all_but_value = False
        else:
            raise TypeError("all_but_meta must be a list of strings."
                            " {} was provided".format(type(all_but_meta)))
        projected_meta = all_but_meta if all_but_value else projected_meta

        projected_regs_exists = False
        if isinstance(projected_regs, list) and \
                all([isinstance(x, str) for x in projected_regs]):
            projected_regs = Some(projected_regs)
            projected_regs_exists = True
        elif projected_regs is None:
            projected_regs = none()
        else:
            raise TypeError("projected_regs must be a list of strings or None."
                            " {} was provided".format(type(projected_regs)))

        if isinstance(new_field_dict, dict):
            regs_ext = []
            expBuild = self.pmg.getNewExpressionBuilder(self.__index)
            for k in new_field_dict.keys():
                item = new_field_dict[k]
                if isinstance(k, str):
                    if isinstance(item, RegField):
                        re = expBuild.createRegionExtension(k, item.getRegionExpression())
                    elif isinstance(item, MetaField):
                        re = expBuild.createRegionExtension(k, item.reMetaNode)
                    elif isinstance(item, int):
                        re = expBuild.createRegionExtension(k, expBuild.getREType("float", str(item)))
                    elif isinstance(item, str):
                        re = expBuild.createRegionExtension(k, expBuild.getREType("string", item))
                    elif isinstance(item, float):
                        re = expBuild.createRegionExtension(k, expBuild.getREType("float", str(item)))
                    else:
                        raise TypeError("Type {} of item of new_field_dict is not valid".format(type(item)))
                    regs_ext.append(re)
                else:
                    raise TypeError("The key of new_field_dict must be a string. "
                                    "{} was provided".format(type(k)))
            regs_ext = Some(regs_ext)
        elif new_field_dict is None:
            regs_ext = none()
        else:
            raise TypeError("new_field_dict must be a dictionary."
                            " {} was provided".format(type(new_field_dict)))

        if isinstance(all_but_regs, list) and \
            all([isinstance(x, str) for x in all_but_regs]):
            if not projected_regs_exists:
                all_but_regs = Some(all_but_regs)
            else:
                raise ValueError("all_but_meta and projected_meta are mutually exclusive")
        elif all_but_regs is None:
            all_but_regs = none()
        else:
            raise TypeError("all_but_regs must be a list of strings."
                            " {} was provided".format(type(all_but_regs)))

        new_index = self.opmng.project(self.__index, projected_meta, meta_ext, all_but_value,
                                       projected_regs, all_but_regs, regs_ext)
        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources, remote_sources=self._remote_sources,
                           meta_profile=self.meta_profile)

    def meta_project(self, attr_list=None, all_but=None, new_attr_dict=None):
        """
        Project the metadata based on a list of attribute names
        
        :param attr_list: list of the metadata fields to select
        :param all_but: list of metadata that must be excluded from the projection.
        :param new_attr_dict: an optional dictionary of the form {'new_field_1': function1,
               'new_field_2': function2, ...} in which every function computes
               the new field based on the values of the others
        :return: a new GMQLDataset

        Notice that if attr_list is specified, all_but cannot be specified and viceversa.

        Examples::

            new_dataset = dataset.meta_project(attr_list=['antibody', 'ID'],
                                               new_attr_dict={'new_meta': dataset['ID'] + 100})

        """
        return self.project(projected_meta=attr_list, new_attr_dict=new_attr_dict, all_but_meta=all_but)

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

            new_dataset = dataset.reg_project(field_list=['peak', 'pvalue'],
                                              new_field_dict={'new_field': dataset.pvalue * dataset['cell_age', 'float']})

        Notice that you can use metadata attributes for building new region fields.
        The only thing to remember when doing so is to define also the type of the output region field
        in the metadata attribute definition (for example, :code:`dataset['cell_age', 'float']` is
        required for defining the new attribute :code:`new_field` as float).
        In particular, the following type names are accepted: 'string', 'char', 'long', 'integer',
        'boolean', 'float', 'double'.

        """
        return self.project(projected_regs=field_list, all_but_regs=all_but, new_field_dict=new_field_dict)

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
        if isinstance(new_attr_dict, dict):
            expBuild = self.pmg.getNewExpressionBuilder(self.__index)
            aggregates = []
            for k in new_attr_dict.keys():
                if isinstance(k, str):
                    item = new_attr_dict[k]
                    if isinstance(item, Aggregate):
                        op_name = item.get_aggregate_name()
                        op_argument = Some(item.get_argument()) if item.is_unary() else none()
                        regsToMeta = expBuild.getRegionsToMeta(op_name, k, op_argument)
                        aggregates.append(regsToMeta)
                    else:
                        raise TypeError("The items in new_reg_fields must be Aggregates."
                                        " {} was provided".format(type(item)))
                else:
                    raise TypeError("The key of new_attr_dict must be a string. "
                                    "{} was provided".format(type(k)))
        else:
            raise TypeError("new_attr_dict must be a dictionary. "
                            "{} was provided".format(type(new_attr_dict)))

        new_index = self.opmng.extend(self.__index, aggregates)
        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources,
                           remote_sources=self._remote_sources, meta_profile=self.meta_profile)

    def cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None, cover_type="normal"):
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

        :param cover_type: the kind of cover variant you want ['normal', 'flat', 'summit', 'histogram']
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
        if isinstance(cover_type, str):
            coverFlag = self.opmng.getCoverTypes(cover_type)
        else:
            raise TypeError("type must be a string. "
                            "{} was provided".format(type(cover_type)))
        if isinstance(minAcc, str):
            minAccParam = self.opmng.getCoverParam(minAcc.lower())
        elif isinstance(minAcc, int):
            minAccParam = self.opmng.getCoverParam(str(minAcc).lower())
        else:
            raise TypeError("minAcc must be a string or an integer. "
                            "{} was provided".format(type(minAcc)))
        if isinstance(maxAcc, str):
            maxAccParam = self.opmng.getCoverParam(maxAcc.lower())
        elif isinstance(maxAcc, int):
            maxAccParam = self.opmng.getCoverParam(str(maxAcc).lower())
        else:
            raise TypeError("maxAcc must be a string or an integer. "
                            "{} was provided".format(type(minAcc)))

        if isinstance(groupBy, list) and \
            all([isinstance(x, str) for x in groupBy]):
            groupBy_result = Some(groupBy)
        elif groupBy is None:
            groupBy_result = none()
        else:
            raise TypeError("groupBy must be a list of string. "
                            "{} was provided".format(type(groupBy)))

        aggregates = []
        if isinstance(new_reg_fields, dict):
            expBuild = self.pmg.getNewExpressionBuilder(self.__index)
            for k in new_reg_fields.keys():
                if isinstance(k, str):
                    item = new_reg_fields[k]
                    if isinstance(item, (SUM, MIN, MAX, AVG, BAG, BAGD,
                                         MEDIAN, COUNT)):
                        op_name = item.get_aggregate_name()
                        op_argument = item.get_argument()
                        if op_argument is None:
                            op_argument = none()
                        else:
                            op_argument = Some(op_argument)
                        regsToReg = expBuild.getRegionsToRegion(op_name, k, op_argument)
                        aggregates.append(regsToReg)
                    else:
                        raise TypeError("The items in new_reg_fields must be Aggregates (SUM, MIN, MAX, AVG, BAG, "
                                        "BAGD, MEDIAN, COUNT)"
                                        " {} was provided".format(type(item)))
                else:
                    raise TypeError("The key of new_reg_fields must be a string. "
                                    "{} was provided".format(type(k)))
        elif new_reg_fields is None:
            pass
        else:
            raise TypeError("new_reg_fields must be a list of dictionary. "
                            "{} was provided".format(type(new_reg_fields)))

        new_index = self.opmng.cover(self.__index, coverFlag, minAccParam, maxAccParam,
                                     groupBy_result, aggregates)
        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources,
                           remote_sources=self._remote_sources, meta_profile=self.meta_profile)

    def normal_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        The normal cover operation as described in :meth:`~.cover`.
        Equivalent to calling::
        
            dataset.cover("normal", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, cover_type="normal")

    def flat_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        Variant of the function :meth:`~.cover` that returns the union of all the regions 
        which contribute to the COVER. More precisely, it returns the contiguous regions 
        that start from the first end and stop at the last end of the regions which would 
        contribute to each region of a COVER.
        
        Equivalent to calling::
        
            cover("flat", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, cover_type="flat")

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
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, cover_type="summit")

    def histogram_cover(self, minAcc, maxAcc, groupBy=None, new_reg_fields=None):
        """
        Variant of the function :meth:`~.cover` that returns all regions contributing to 
        the COVER divided in different (contiguous) parts according to their accumulation 
        index value (one part for each different accumulation value), which is assigned to 
        the AccIndex region attribute.
        
        Equivalent to calling::
        
            cover("histogram", ...)
        """
        return self.cover(minAcc, maxAcc, groupBy, new_reg_fields, cover_type="histogram")

    def join(self, experiment, genometric_predicate, output="LEFT", joinBy=None,
             refName="REF", expName="EXP", left_on=None, right_on=None):
        """ The JOIN operator takes in input two datasets, respectively known as anchor
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
        :param left_on: list of region fields of the reference on which the join must be performed
        :param right_on: list of region fields of the experiment on which the join must be performed
        :return: a new GMQLDataset
        
        An example of usage::
            
            import gmql as gl
            
            # anchor_dataset and experiment_dataset are created
            
            result_dataset = anchor_dataset.join(experiment=experiment_dataset, 
                                                    genometric_predicate=[gl.MD(1), gl.DGE(120000)], 
                                                    output="right")
        """

        if isinstance(experiment, GMQLDataset):
            other_idx = experiment.__index
        else:
            raise TypeError("experiment must be a GMQLDataset. "
                            "{} was provided".format(type(experiment)))
        if isinstance(genometric_predicate, list) and \
            all([isinstance(x, GenometricCondition) for x in genometric_predicate]):
            regionJoinCondition = self.opmng.getRegionJoinCondition(list(map(lambda x: x.get_gen_condition(),
                                                                             genometric_predicate)))
        else:
            raise TypeError("genometric_predicate must be a list og GenometricCondition. "
                            "{} was found".format(type(genometric_predicate)))

        if isinstance(output, str):
            regionBuilder = self.opmng.getRegionBuilderJoin(output)
        else:
            raise TypeError("output must be a string. "
                            "{} was provided".format(type(output)))

        if not isinstance(expName, str):
            raise TypeError("expName must be a string. {} was provided".format(type(expName)))

        if not isinstance(refName, str):
            raise TypeError("refName must be a string. {} was provided".format(type(expName)))

        if isinstance(joinBy, list) and \
            all([isinstance(x, str) for x in joinBy]):
            metaJoinCondition = Some(self.opmng.getMetaJoinCondition(joinBy))
        elif joinBy is None:
            metaJoinCondition = none()
        else:
            raise TypeError("joinBy must be a list of strings. "
                            "{} was found".format(type(joinBy)))

        left_on_exists = False
        left_on_len = 0
        if isinstance(left_on, list) and \
            all([isinstance(x, str) for x in left_on]):
            left_on_len = len(left_on)
            left_on = Some(left_on)
            left_on_exists = True
        elif left_on is None:
            left_on = none()
        else:
            raise TypeError("left_on must be a list of strings. "
                            "{} was provided".format(type(left_on)))

        if isinstance(right_on, list) and \
            all([isinstance(x, str)] for x in right_on) and \
            left_on_exists and len(right_on) == left_on_len:
            right_on = Some(right_on)
        elif right_on is None and not left_on_exists:
            right_on = none()
        else:
            raise TypeError("right_on must be a list of strings. "
                            "{} was provided".format(type(right_on)))

        new_index = self.opmng.join(self.__index, other_idx,
                                    metaJoinCondition, regionJoinCondition, regionBuilder,
                                    refName, expName, left_on, right_on)
        new_local_sources, new_remote_sources = self.__combine_sources(self, experiment)
        new_location = self.__combine_locations(self, experiment)
        return GMQLDataset(index=new_index, location=new_location,
                           local_sources=new_local_sources,
                           remote_sources=new_remote_sources, meta_profile=self.meta_profile)

    def map(self, experiment, new_reg_fields=None, joinBy=None, refName="REF", expName="EXP"):
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
        if isinstance(experiment, GMQLDataset):
            other_idx = experiment.__index
        else:
            raise TypeError("experiment must be a GMQLDataset. "
                            "{} was provided".format(type(experiment)))

        aggregates = []
        if isinstance(new_reg_fields, dict):
            expBuild = self.pmg.getNewExpressionBuilder(experiment.__index)
            for k in new_reg_fields.keys():
                if isinstance(k, str):
                    item = new_reg_fields[k]
                    if isinstance(item, (SUM, MIN, MAX, AVG, BAG, BAGD,
                                         MEDIAN, COUNT)):
                        op_name = item.get_aggregate_name()
                        op_argument = item.get_argument()
                        if op_argument is None:
                            op_argument = none()
                        else:
                            op_argument = Some(op_argument)
                        regsToReg = expBuild.getRegionsToRegion(op_name, k, op_argument)
                        aggregates.append(regsToReg)
                    else:
                        raise TypeError("The items in new_reg_fields must be Aggregates (SUM, MIN, MAX, AVG, BAG, BAGD, "
                                        "MEDIAN, COUNT)"
                                        " {} was provided".format(type(item)))
                else:
                    raise TypeError("The key of new_reg_fields must be a string. "
                                    "{} was provided".format(type(k)))
        elif new_reg_fields is None:
            pass
        else:
            raise TypeError("new_reg_fields must be a list of dictionary. "
                            "{} was provided".format(type(new_reg_fields)))

        if isinstance(joinBy, list) and \
            all([isinstance(x, str) for x in joinBy]):
            metaJoinCondition = Some(self.opmng.getMetaJoinCondition(joinBy))
        elif joinBy is None:
            metaJoinCondition = none()
        else:
            raise TypeError("joinBy must be a list of strings. "
                            "{} was found".format(type(joinBy)))

        if not isinstance(expName, str):
            raise TypeError("expName must be a string. {} was provided".format(type(expName)))

        if not isinstance(refName, str):
            raise TypeError("refName must be a string. {} was provided".format(type(expName)))

        new_index = self.opmng.map(self.__index, other_idx, metaJoinCondition,
                                   aggregates, refName, expName)
        new_local_sources, new_remote_sources = self.__combine_sources(self, experiment)
        new_location = self.__combine_locations(self, experiment)
        return GMQLDataset(index=new_index, location=new_location,
                           local_sources=new_local_sources,
                           remote_sources=new_remote_sources,
                           meta_profile=self.meta_profile)

    def order(self, meta=None, meta_ascending=None, meta_top=None, meta_k=None,
              regs=None, regs_ascending=None, region_top=None, region_k=None):
        """ The ORDER operator is used to order either samples, sample regions, or both,
        in a dataset according to a set of metadata and/or region attributes, and/or 
        region coordinates. The number of samples and their regions in the output dataset 
        is as in the input dataset, as well as their metadata and region attributes 
        and values, but a new ordering metadata and/or region attribute is added with 
        the sample or region ordering value, respectively. 
        
        :param meta: list of metadata attributes
        :param meta_ascending: list of boolean values (True = ascending, False = descending)
        :param meta_top: "top", "topq" or "topp" or None
        :param meta_k: a number specifying how many results to be retained
        :param regs: list of region attributes
        :param regs_ascending: list of boolean values (True = ascending, False = descending)
        :param region_top: "top", "topq" or "topp" or None
        :param region_k: a number specifying how many results to be retained
        :return: a new GMQLDataset
        """
        meta_exists = False
        meta_len = 0
        if isinstance(meta, list) and \
            all([isinstance(x, str) for x in meta]):
            meta_exists = True
            meta_len = len(meta)
            meta = Some(meta)
        elif meta is None:
            meta = none()
        else:
            raise TypeError("meta must be a list of strings. "
                            "{} was provided".format(type(meta)))

        if isinstance(meta_ascending, list) and \
            all([isinstance(x, bool) for x in meta_ascending]) and \
            meta_exists and meta_len == len(meta_ascending):
            meta_ascending = Some(meta_ascending)
        elif meta_ascending is None:
            if meta_exists:
                # by default meta_ascending is all True
                meta_ascending = Some([True for _ in range(meta_len)])
            else:
                meta_ascending = none()
        else:
            raise TypeError("meta ascending must be a list of booleans having the same size "
                            "of meta. {} was provided".format(type(meta_ascending)))

        regs_exists = False
        regs_len = 0
        if isinstance(regs, list) and \
                all([isinstance(x, str) for x in regs]):
            regs_exists = True
            regs_len = len(regs)
            regs = Some(regs)
        elif regs is None:
            regs = none()
        else:
            raise TypeError("regs must be a list of strings. "
                            "{} was provided".format(type(regs)))

        if isinstance(regs_ascending, list) and \
            all([isinstance(x, bool) for x in regs_ascending]) and \
                regs_exists and regs_len == len(regs_ascending):
            regs_ascending = Some(regs_ascending)
        elif regs_ascending is None:
            if regs_exists:
                # by default regs_ascending is all True
                regs_ascending = Some([True for _ in range(regs_len)])
            else:
                regs_ascending = none()
        else:
            raise TypeError("meta regs_ascending must be a list of booleans having the same size "
                            "of regs. {} was provided".format(type(regs_ascending)))

        meta_top_exists = False
        if isinstance(meta_top, str):
            if meta_exists:
                meta_top = Some(meta_top)
                meta_top_exists = True
            else:
                raise ValueError("meta_top must be defined only when meta is defined")
        elif meta_top is None:
            meta_top = none()
        else:
            raise TypeError("meta_top must be a string. {} was provided".format(type(meta_top)))

        if isinstance(meta_k, int) and \
                meta_top_exists:
            meta_k = Some(meta_k)
        elif meta_k is None and \
                not meta_top_exists:
            meta_k = none()
        else:
            raise TypeError("meta_k must be an integer and should be provided together with a "
                            "value of meta_top. {} was provided".format(type(meta_k)))

        region_top_exists = False
        if isinstance(region_top, str):
            if regs_exists:
                region_top = Some(region_top)
                region_top_exists = True
            else:
                raise ValueError("region_top must be defined only when regs is defined")
        elif region_top is None:
            region_top = none()
        else:
            raise TypeError("region_top must be a string. {} was provided".format(type(region_top)))

        if isinstance(region_k, int) and \
                region_top_exists:
            region_k = Some(region_k)
        elif region_k is None and \
                not region_top_exists:
            region_k = none()
        else:
            raise TypeError("region_k must be an integer and should be provided together with a "
                            "value of region_top. {} was provided".format(type(region_k)))

        new_index = self.opmng.order(self.__index, meta, meta_ascending, meta_top, meta_k,
                                     regs, regs_ascending, region_top, region_k)
        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources,
                           remote_sources=self._remote_sources,
                           meta_profile=self.meta_profile)

    def difference(self, other, joinBy=None, exact=False):
        """ DIFFERENCE is a binary, non-symmetric operator that produces one sample
        in the result for each sample of the first operand, by keeping the same
        metadata of the first operand sample and only those regions (with their
        schema and values) of the first operand sample which do not intersect with
        any region in the second operand sample (also known as negative regions)
        
        :param other: GMQLDataset
        :param joinBy: list of metadata attributes
        :param exact: boolean
        :return: a new GMQLDataset
        """

        if isinstance(other, GMQLDataset):
            other_idx = other.__index
        else:
            raise TypeError("other must be a GMQLDataset. "
                            "{} was provided".format(type(other)))

        if isinstance(joinBy, list) and \
            all([isinstance(x, str) for x in joinBy]):
            metaJoinCondition = Some(self.opmng.getMetaJoinCondition(joinBy))
        elif joinBy is None:
            metaJoinCondition = none()
        else:
            raise TypeError("joinBy must be a list of strings. "
                            "{} was provided".format(type(joinBy)))

        if not isinstance(exact, bool):
            raise TypeError("exact must be a boolean. "
                            "{} was provided".format(type(exact)))

        new_index = self.opmng.difference(self.__index, other_idx, metaJoinCondition, exact)

        new_local_sources, new_remote_sources = self.__combine_sources(self, other)
        new_location = self.__combine_locations(self, other)
        return GMQLDataset(index=new_index, location=new_location,
                           local_sources=new_local_sources,
                           remote_sources=new_remote_sources,
                           meta_profile=self.meta_profile)

    def union(self, other, left_name="LEFT", right_name="RIGHT"):
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

        if not isinstance(left_name, str) or \
            not isinstance(right_name, str):
            raise TypeError("left_name and right_name must be strings. "
                            "{} - {} was provided".format(type(left_name), type(right_name)))

        if isinstance(other, GMQLDataset):
            other_idx = other.__index
        else:
            raise TypeError("other must be a GMQLDataset. "
                            "{} was provided".format(type(other)))

        if len(left_name) == 0 or len(right_name) == 0:
            raise ValueError("left_name and right_name must not be empty")
        new_index = self.opmng.union(self.__index, other_idx, left_name, right_name)
        new_local_sources, new_remote_sources = self.__combine_sources(self, other)
        new_location = self.__combine_locations(self, other)
        return GMQLDataset(index=new_index, location=new_location,
                           local_sources=new_local_sources,
                           remote_sources=new_remote_sources,
                           meta_profile=self.meta_profile)

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

        if isinstance(groupBy, list) and \
            all([isinstance(x, str) for x in groupBy]):
            groupBy = Some(groupBy)
        elif groupBy is None:
            groupBy = none()
        else:
            raise TypeError("groupBy must be a list of strings. "
                            "{} was provided".format(type(groupBy)))

        new_index = self.opmng.merge(self.__index, groupBy)
        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources,
                           remote_sources=self._remote_sources,
                           meta_profile=self.meta_profile)

    def group(self, meta=None, meta_aggregates=None, regs=None,
              regs_aggregates=None, meta_group_name="_group"):
        """ The GROUP operator is used for grouping both regions and/or metadata of input
        dataset samples according to distinct values of certain attributes (known as grouping
        attributes); new grouping attributes are added to samples in the output dataset,
        storing the results of aggregate function evaluations over metadata and/or regions
        in each group of samples.
        Samples having missing values for any of the grouping attributes are discarded.


        :param meta: (optional) a list of metadata attributes
        :param meta_aggregates: (optional) {'new_attr': fun}
        :param regs: (optional) a list of region fields
        :param regs_aggregates: {'new_attr': fun}
        :param meta_group_name: (optional) the name to give to the group attribute in the
               metadata
        :return: a new GMQLDataset
        """

        if isinstance(meta, list) and \
            all([isinstance(x, str) for x in meta]):
            meta = Some(meta)
        elif meta is None:
            meta = none()
        else:
            raise TypeError("meta must be a list of strings. "
                            "{} was provided".format(type(meta)))
        expBuild = self.pmg.getNewExpressionBuilder(self.__index)
        if isinstance(meta_aggregates, dict):
            metaAggregates = []
            for k in meta_aggregates:
                if isinstance(k, str):
                    item = meta_aggregates[k]
                    if isinstance(item, (SUM, MIN, MAX, AVG, BAG,
                                         BAGD, STD, MEDIAN, COUNTSAMP)):
                        functionName = item.get_aggregate_name()
                        argument = item.get_argument()
                        if argument is None:
                            argument = none()
                        else:
                            argument = Some(argument)
                        metaAggregates.append(expBuild.createMetaAggregateFunction(functionName,
                                                                                   k, argument))
                    else:
                        raise TypeError("the item of the dictionary must be an Aggregate of the following: "
                                        "SUM, MIN, MAX, AVG, BAG, BAGD, STD, COUNTSAMP. "
                                        "{} was provided".format(type(item)))
                else:
                    raise TypeError("keys of meta_aggregates must be string. "
                                    "{} was provided".format(type(k)))
            metaAggregates = Some(metaAggregates)
        elif meta_aggregates is None:
            metaAggregates = none()
        else:
            raise TypeError("meta_aggregates must be a dictionary of Aggregate functions. "
                            "{} was provided".format(type(meta_aggregates)))
        if isinstance(regs, list) and \
            all([isinstance(x, str) for x in regs]):
            regs = Some(regs)
        elif regs is None:
            regs = none()
        else:
            raise TypeError("regs must be a list of strings. "
                            "{} was provided".format(type(regs)))

        if isinstance(regs_aggregates, dict):
            regionAggregates = []
            for k in regs_aggregates.keys():
                if isinstance(k, str):
                    item = regs_aggregates[k]
                    if isinstance(item, (SUM, MIN, MAX, AVG, BAG, BAGD,
                                         MEDIAN, COUNT)):
                        op_name = item.get_aggregate_name()
                        op_argument = item.get_argument()
                        if op_argument is None:
                            op_argument = none()
                        else:
                            op_argument = Some(op_argument)
                        regsToReg = expBuild.getRegionsToRegion(op_name, k, op_argument)
                        regionAggregates.append(regsToReg)
                    else:
                        raise TypeError("the item of the dictionary must be an Aggregate of the following: "
                                        "SUM, MIN, MAX, AVG, BAG, BAGD, MEDIAN, COUNT. "
                                        "{} was provided".format(type(item)))
                else:
                    raise TypeError("The key of new_reg_fields must be a string. "
                                    "{} was provided".format(type(k)))
            regionAggregates = Some(regionAggregates)
        elif regs_aggregates is None:
            regionAggregates = none()
        else:
            raise TypeError("new_reg_fields must be a list of dictionary. "
                            "{} was provided".format(type(regs_aggregates)))

        if isinstance(meta_group_name, str):
            pass
        else:
            raise TypeError("meta_group_name must be a string. "
                            "{} was provided".format(type(meta_group_name)))

        new_index = self.opmng.group(self.__index, meta, metaAggregates, meta_group_name, regs, regionAggregates)

        return GMQLDataset(index=new_index, location=self.location,
                           local_sources=self._local_sources,
                           remote_sources=self._remote_sources,
                           meta_profile=self.meta_profile)

    def meta_group(self, meta, meta_aggregates=None):
        """ Group operation only for metadata. For further information check :meth:`~.group`
        """
        return self.group(meta=meta, meta_aggregates=meta_aggregates)

    def regs_group(self, regs, regs_aggregates=None):
        """ Group operation only for region data. For further information check :meth:`~.group`
        """
        return self.group(regs=regs, regs_aggregates=regs_aggregates)
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
        current_mode = get_mode()
        new_index = self.__modify_dag(current_mode)
        if current_mode == 'local':
            return Materializations.materialize_local(new_index, output_path)
        elif current_mode == 'remote':
            return Materializations.materialize_remote(new_index, output_name, output_path, all_load)
        else:
            raise ValueError("Current mode is not defined. {} given".format(current_mode))

    def take(self, n=5):
        """ Returns a small set of regions and metadata from a query. It is supposed to
        be used for debugging purposes or for data exploration.

        :param n: how many samples to retrieve
        :return: a GDataframe
        """
        if n <= 0:
            raise ValueError("n must be a positive number. {} was given".format(n))

        collected = self.pmg.take(self.__index, n)
        regs = MemoryLoader.load_regions(collected)
        meta = MemoryLoader.load_metadata(collected)
        result = GDataframe.GDataframe(regs, meta)
        return result

    def _get_serialized_dag(self):
        serialized_dag = self.pmg.serializeVariable(self.__index)
        return serialized_dag

    @staticmethod
    def __combine_sources(d1, d2):
        if (not isinstance(d1, GMQLDataset)) or (not isinstance(d2, GMQLDataset)):
            raise TypeError("The function takes only GMQLDataset")
        local_sources_1 = d1._local_sources
        remote_sources_1 = d1._remote_sources
        local_sources_2 = d2._local_sources
        remote_sources_2 = d2._remote_sources

        new_local_sources = list(set(local_sources_1 + local_sources_2))
        new_remote_sources = list(set(remote_sources_1 + remote_sources_2))

        return new_local_sources, new_remote_sources

    @staticmethod
    def __combine_locations(d1, d2):
        if (not isinstance(d1, GMQLDataset)) or (not isinstance(d2, GMQLDataset)):
            raise TypeError("The function takes only GMQLDataset")

        location_1 = d1.location
        location_2 = d2.location

        if location_1 == location_2:
            return location_1
        else:
            return "mixed"

    def __modify_dag(self, mode):
        remote_manager = get_remote_manager()
        index = self.__index
        pmg = get_python_manager()
        sources = _get_source_table()
        # create a new id having the exact same DAG inside, for modification
        new_index = pmg.cloneVariable(index)
        if mode == "local":
            for d in self._remote_sources:
                # for each remote source, we have to download it locally in a temporary folder
                d_sources = sources.get_source(id=d)
                local = d_sources[LOCAL]
                remote = d_sources[REMOTE]
                if local is None:
                    new_name = get_new_dataset_tmp_folder()
                    remote_manager.download_dataset(dataset_name=remote, local_path=new_name, how="stream")
                    sources.modify_source(id=d, local=new_name)
                else:
                    new_name = local
                pmg.modify_dag_source(new_index, str(d), new_name)
            for d in self._local_sources:
                # for each local source, just take its path
                d_sources = sources.get_source(id=d)
                local = d_sources[LOCAL]
                if local is None:
                    raise ValueError("Impossible state. Local source must have a local path")
                else:
                    pmg.modify_dag_source(new_index, str(d), local)
        elif mode == "remote":
            for d in self._local_sources:
                # for each local source, we have to upload it remotely
                d_sources = sources.get_source(id=d)
                local = d_sources[LOCAL]
                remote = d_sources[REMOTE]
                parser = d_sources[PARSER]
                if remote is None:
                    new_name = "LOCAL_" + get_unique_identifier()
                    schema_dir = get_new_dataset_tmp_folder()
                    os.makedirs(schema_dir)
                    schema_tmp_path = os.path.join(schema_dir, new_name + ".schema")
                    parserToXML(parser, new_name, schema_tmp_path)
                    remote_manager.upload_dataset(dataset=local, dataset_name=new_name, schema_path=schema_tmp_path)
                    sources.modify_source(id=d, remote=new_name)
                else:
                    new_name = remote
                pmg.modify_dag_source(new_index, str(d), new_name)
            for d in self._remote_sources:
                d_sources = sources.get_source(id=d)
                remote = d_sources[REMOTE]
                if remote is None:
                    raise ValueError("Impossible state. Remote source must have a remote name")
                else:
                    pmg.modify_dag_source(new_index, str(d), remote)
        else:
            raise ValueError("Unknown mode {}".format(mode))

        return new_index
