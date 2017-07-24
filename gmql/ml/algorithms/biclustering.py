"""
Biclustering algorithms.

"""

from sklearn.cluster.bicluster import SpectralBiclustering
from sklearn.cluster.bicluster import SpectralCoclustering
from sklearn.metrics import consensus_score


class Biclustering:

    def __init__(self, model):
        self.model = model

    @classmethod
    def spectral_biclustering(cls, *args):
        """
            Wrapper method for the spectral_biclustering algorithm
            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Biclustering object
        """

        model = SpectralBiclustering(*args)
        return cls(model)

    @classmethod
    def spectral_coclustering(cls, *args):
        """
        Wrapper method for the spectral_coclustering algorithm
        :param args: the arguments to be sent to the sci-kit implementation
        :return: returns the Biclustering object
        """

        model = SpectralCoclustering(*args)
        return cls(model)

    def fit(self, data):
        """
        Performs biclustering
        :param data: Data to be fit
        :return: 
        """
        self.model.fit(data)

    def retrieve_bicluster(self, df, row_no, column_no):
        """
        Extracts the bicluster at the given row bicluster number and the column bicluster number from the input dataframe.
        :param df: the input dataframe whose values were biclustered
        :param row_no: the number of the row bicluster
        :param column_no: the number of the column bicluster
        :return: the extracted bicluster from the dataframe
        """
        res = df[self.model.biclusters_[0][row_no]]
        bicluster = res[res.columns[self.model.biclusters_[1][column_no]]]
        return bicluster

    def bicluster_similarity(self, reference_model):
        """
        Calcolates the similarity between the current model of biclusters and the reference model of biclusters
        :param reference_model: The reference model of biclusters
        :return: Returns the consensus score(Hochreiter et. al., 2010), i.e. the similarity of two sets of biclusters.
        """
        similarity_score = consensus_score(self.model.biclusters_, reference_model.biclusters_)
        return similarity_score
