import pandas as pd
import os
import xml.etree.ElementTree
from tqdm import tqdm


class Parser:
    """

    General purpose parser for the (tab separated) output of the MAP operations of GMQL.
    This parser allows the parsing of a subset of region and meta data of interest to improve performance

    """

    def __init__(self, path):
        self.path = path
        self.schema = self._get_schema_file("schema", path)
        return

    @staticmethod
    def get_sample_id(path):
        """
        Returns the id of the sample (the filename) file given a path
        :param path: Path of the sample file
        :return: The resulting sample id
        """
        sp = path.split('/')
        file_name = sp[-1]
        return file_name.split('.')[0]

    @staticmethod
    def _get_files(extension, path):
        """
        Returns a sorted list of all of the files having the same extension under the same directory
        :param extension: the extension of the data files such as 'gdm'
        :param path: path to the folder containing the files
        :return: sorted list of files
        """
        # retrieves the files sharing the same extension
        files = []
        for file in os.listdir(path):
            if file.endswith(extension):
                files.append(os.path.join(path, file))
        return sorted(files)

    @staticmethod
    def _get_schema_file(extension, path):
        """
        Returns the schema file
        :param extension: extension of the schema file usually .schema
        :param path: path to the folder containing the schema file
        :return: the path to the schema file
        """
        for file in os.listdir(path):
            if file.endswith(extension):
                return os.path.join(path, file)

    @staticmethod
    def parse_schema(schema_file):
        """
        parses the schema file and returns the columns that are later going to represent the columns of the genometric space dataframe
        :param schema_file: the path to the schema file
        :return: the columns of the schema file
        """
        e = xml.etree.ElementTree.parse(schema_file)
        root = e.getroot()
        cols = []
        for elem in root.findall(".//{http://genomic.elet.polimi.it/entities}field"):  # XPATH
            cols.append(elem.text)
        return cols

    @staticmethod
    def _get_sample_name(path):
        """
        Returns the sample name given the path to the sample file
        :param path: path to the sample file
        :return: name of the sample
        """
        sp = path.split('/')
        file_name = sp[-1]
        return file_name.split('.')[0]

    def parse_single_meta(self, fname, selected_meta_data):
        """
        Parses a single meta data file
        :param fname: name of the file
        :param selected_meta_data: If not none then only the specified columns of metadata are parsed
        :return: the resulting pandas series
        """
        # reads a meta data file into a dataframe
        columns = []
        data = []
        with open(fname) as f:
            for line in f:
                splitted = line.split('\t')
                columns.append(splitted[0])
                data.append(splitted[1].split('\n')[0])  # to remove the \n values
        df = pd.DataFrame(data=data, index=columns)
        df = df.T
        sample = self._get_sample_name(fname)
        if selected_meta_data:  # if null then keep all the columns
            try:
                df = df[selected_meta_data]
            except:
                pass
        df['sample'] = sample
        return df

    def parse_meta(self, selected_meta_data):
        """
        Parses all of the metadata files
        :param selected_meta_data: if specified then only the columns that are contained here are going to be parsed
        :return:
        """
        # reads all meta data files
        files = self._get_files("meta", self.path)
        df = pd.DataFrame()
        print("Parsing the metadata files...")
        for f in tqdm(files):
            data = self.parse_single_meta(f, selected_meta_data)
            if data is not None:
                df = pd.concat([df, data], axis=0)
        df.index = df['sample']
        #
        # df = df.drop('sample', 1)  # 1 for the columns
        return df

    def parse_single_data(self, path, cols, selected_region_data, selected_values, full_load):
        """
        Parses a single region data file
        :param path: path to the file
        :param cols: the column names coming from the schema file
        :param selected_region_data: the selected of the region data to be parsed
        In most cases the analyst only needs a small subset of the region data
        :param selected_values: the selected values to be put in the matrix cells
        :param full_load: Specifies the method of parsing the data. If False then parser omits the parsing of zero(0)
            values in order to speed up and save memory. However, while creating the matrix, those zero values are going to be put into the matrix.
            (unless a row contains "all zero columns". This parsing is strongly recommended for sparse datasets.
            If the full_load parameter is True then all the zero(0) data are going to be read.
        :return: the dataframe containing the region data
        """
        # reads a sample file
        df = pd.read_table(path, engine='c', sep="\t", lineterminator="\n", header=None)
        df.columns = cols  # column names from schema
        df = df[selected_region_data]

        if not full_load:
            if type(selected_values) is list:
                df_2 = pd.DataFrame(dtype=float)
                for value in selected_values:
                    df_3 = df.loc[df[value] != 0]
                    df_2 = pd.concat([df_2, df_3], axis=0)
                df = df_2
            else:
                df = df.loc[df[selected_values] != 0]

        sample = self._get_sample_name(path)
        df['sample'] = sample
        return df

    def parse_data(self, selected_region_data, selected_values, full_load=False, extension="gdm"):
        """
        Parses all of the region data
        :param selected_region_data: the columns of region data that are needed
        :param selected_values: the selected values to be put in the matrix cells
        :param full_load: Specifies the method of parsing the data. If False then parser omits the parsing of zero(0)
            values in order to speed up and save memory. However, while creating the matrix, those zero values are going to be put into the matrix.
            (unless a row contains "all zero columns". This parsing is strongly recommended for sparse datasets.
            If the full_load parameter is True then all the zero(0) data are going to be read.
        :param extension: the extension of the region data files that are going to be parsed.
        :return: the resulting region dataframe
        """
        regions = list(selected_region_data)
        if type(selected_values) is list:
            regions.extend(selected_values)
        else:
            regions.append(selected_values)

        files = self._get_files(extension, self.path)
        df = pd.DataFrame(dtype=float)

        cols = self.parse_schema(self.schema)
        print("Parsing the data files...")
        list_of_data = []
        for f in tqdm(files):
            data = self.parse_single_data(f, cols, regions, selected_values, full_load)
            list_of_data.append(data)
        print("pre-concat")
        df = pd.concat(list_of_data)
        return df
