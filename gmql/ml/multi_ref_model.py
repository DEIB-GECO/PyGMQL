from genometric_space import GenometricSpace
from parser.parser import Parser
import pandas as pd
import warnings
import numpy as np


class MultiRefModel:
    """
    GenometricSpace class to represent data that are mapped with multiple references
    
    """

    def __init__(self):
        """
        Constructor
        """
        self.data_model = []
        return

    def load(self, path, genes_uuid, regs=['chr', 'left', 'right', 'strand'], meta=[], values=[], full_load=False):
        """
        Loads the multi referenced mapped data from the file system
        
        :param path: The path to the files
        :param genes_uuid: The unique identifier metadata column name to separate the data by the number of references
        :param regs: The region data that are to be analyzed
        :param meta: The metadata that are to be analyzed
        :param values: The values to fill the matrix
        :param full_load: Specifies the method of parsing the data. If False then parser omits the parsing of zero(0) 
        values in order to speed up and save memory. However, while creating the matrix, those zero values are going to be put into the matrix.
        (unless a row contains "all zero columns". This parsing is strongly recommended for sparse datasets.
        If the full_load parameter is True then all the zero(0) data are going to be read.
        :return: 
        """

        if not full_load:
            warnings.warn("\n\n You are using the optimized loading technique. "
                          "All-zero rows are not going to be loaded into memory. "
                          "To load all the data please set the full_load parameter equal to True.")
        p = Parser(path)
        all_meta_data = p.parse_meta(meta)
        all_data = p.parse_data(regs, values, full_load)

        all_data = pd.pivot_table(all_data,
                                   values=values, columns=regs, index=['sample'],
                                   fill_value=0)

        group1 = all_meta_data.groupby([genes_uuid]).count()
        for g in group1.index.values:
            series = all_meta_data[genes_uuid] == g
            m = (all_meta_data[series])
            d = (all_data.loc[series]).dropna(axis=1, how='all')  # not to show the NaN data
            self.data_model.append(GenometricSpace.from_memory(d, m))
            self.all_meta_data = all_meta_data

    def merge(self, samples_uuid):
        """
        The method to merge the datamodels belonging to different references
        :param samples_uuid: The unique identifier metadata column name to identify the identical samples having different references
        :return: Returns the merged dataframe
        """

        all_meta_data = pd.DataFrame()
        for dm in self.data_model:
            all_meta_data = pd.concat([all_meta_data, dm.meta], axis=0)

        group = all_meta_data.groupby([samples_uuid])['sample']
        sample_sets = group.apply(list).values

        merged_df = pd.DataFrame()
        multi_index = list(map(list, zip(*sample_sets)))
        multi_index_names = list(range(0, len(sample_sets[0])))
        i = 1
        for pair in sample_sets:
            i += 1
            numbers = list(range(0, len(pair)))
            df_temp = pd.DataFrame()
            for n in numbers:
                try:  # data.loc[pair[n]] may not be found due to the fast loading (full_load = False)
                    df_temp = pd.concat([df_temp, self.data_model[n].data.loc[pair[n]]], axis=1)
                except:
                    pass
            merged_df = pd.concat([merged_df, df_temp.T.bfill().iloc[[0]]], axis=0)

        multi_index = np.asarray(multi_index)
        multi_index = pd.MultiIndex.from_arrays(multi_index, names=multi_index_names)
        merged_df.index = multi_index
        return merged_df

    def compact_view(self, merged_data, selected_meta, reference_no):
        """
        Creates and returns the compact view where the index of the dataframe is a multi index of the selected metadata.
        Side effect: Alters the merged_data parameter
        :param merged_data: The merged data that is to be used to create the compact view
        :param selected_meta: The selected metadata to create the multi index
        :param reference_no: The reference number that the metadata are going to be taken
        :return: Returns the multi-indexed dataframe w.r.t. the selected metadata
        """
        meta_names = list(selected_meta)
        meta_index = []

        for x in meta_names:
            meta_index.append(self.all_meta_data.ix[merged_data.index.get_level_values(reference_no)][x].values)
        meta_index = np.asarray(meta_index)
        multi_meta_index = pd.MultiIndex.from_arrays(meta_index, names=meta_names)
        merged_data.index = multi_meta_index
        return merged_data

