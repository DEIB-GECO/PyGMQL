"""
Clustering algorithms

"""

from sklearn.cluster import *
from sklearn.metrics.cluster import *
from pyclustering.cluster.xmeans import xmeans, splitting_type
from pyclustering.cluster.clarans import clarans
from pyclustering.cluster.rock import rock
from pyclustering.cluster.optics import optics
from scipy.spatial.distance import cdist, pdist
from matplotlib import pyplot as plt

import pandas as pd
import numpy as np

class Clustering:
    """
    The class that contains the algorithms and related methods
    """
    def __init__(self, model):
        self.model = model

    @classmethod
    def xmeans(cls, initial_centers=None, kmax=20, tolerance=0.025, criterion=splitting_type.BAYESIAN_INFORMATION_CRITERION, ccore=False):
        """
        Constructor of the x-means clustering.rst algorithm

        :param initial_centers: Initial coordinates of centers of clusters that are represented by list: [center1, center2, ...]
        Note: The dimensions of the initial centers should be same as of the dataset.
        :param kmax: Maximum number of clusters that can be allocated.
        :param tolerance: Stop condition for each iteration: if maximum value of change of centers of clusters is less than tolerance than algorithm will stop processing
        :param criterion: Type of splitting creation.
        :param ccore: Defines should be CCORE (C++ pyclustering library) used instead of Python code or not.
        :return: returns the clustering.rst object
        """
        model = xmeans(None, initial_centers, kmax, tolerance, criterion, ccore)
        return cls(model)

    @classmethod
    def clarans(cls, number_clusters, num_local, max_neighbour):
        """
        Constructor of the CLARANS clustering.rst algorithm

        :param number_clusters: the number of clusters to be allocated
        :param num_local: the number of local minima obtained (amount of iterations for solving the problem).
        :param max_neighbour: the number of local minima obtained (amount of iterations for solving the problem).
        :return: the resulting clustering.rst object
        """
        model = clarans(None, number_clusters, num_local, max_neighbour)
        return cls(model)

    @classmethod
    def rock(cls, data, eps, number_clusters, threshold=0.5, ccore=False):
        """
        Constructor of the ROCK cluster analysis algorithm

        :param eps: Connectivity radius (similarity threshold), points are neighbors if distance between them is less than connectivity radius
        :param number_clusters: Defines number of clusters that should be allocated from the input data set
        :param threshold: Value that defines degree of normalization that influences on choice of clusters for merging during processing
        :param ccore: Defines should be CCORE (C++ pyclustering library) used instead of Python code or not.
        :return: The resulting clustering.rst object
        """
        data = cls.input_preprocess(data)
        model = rock(data, eps, number_clusters, threshold, ccore)
        return cls(model)

    @staticmethod
    def input_preprocess(data):
        if isinstance(data, pd.DataFrame):
            data = data.values.tolist()
        elif isinstance(data, np.ndarray):  # in case data is already in the matrix form
            data = data.tolist()
        return data

    @classmethod
    def optics(cls, data, eps, minpts, ccore=False):
        """
        Constructor of OPTICS clustering.rst algorithm

        :param data: Input data that is presented as a list of points (objects), where each point is represented by list or tuple
        :param eps: Connectivity radius between points, points may be connected if distance between them less than the radius
        :param minpts: Minimum number of shared neighbors that is required for establishing links between points
        :param amount_clusters: Optional parameter where amount of clusters that should be allocated is specified.
                    In case of usage 'amount_clusters' connectivity radius can be greater than real, in other words, there is place for mistake
                    in connectivity radius usage.
        :param ccore: if True than DLL CCORE (C++ solution) will be used for solving the problem
        :return: the resulting clustering.rst object
        """
        data = cls.input_preprocess(data)
        model = optics(data, eps, minpts)
        return cls(model)


    @classmethod
    def kmeans(cls, *args):
        """
            Constructor of the k-means clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = KMeans(*args)
        return cls(model)

    @classmethod
    def affinity_propagation(cls, *args):
        """
            Constructor of the affinity propagation clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = AffinityPropagation(*args)
        return cls(model)

    @classmethod
    def hierarchical(cls, *args):
        """
            Constructor of the agglomerative clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = AgglomerativeClustering(*args)
        return cls(model)

    @classmethod
    def birch(cls, *args):
        """
            Constructor of the birch clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = Birch(*args)
        return cls(model)

    @classmethod
    def dbscan(cls, *args):
        """
            Constructor of the DBSCAN clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = DBSCAN(*args)
        return cls(model)

    @classmethod
    def feature_agglomeration(cls, *args):
        """
            Constructor of the feature agglomeration clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = FeatureAgglomeration(*args)
        return cls(model)

    @classmethod
    def mini_batch_kmeans(cls, *args):
        """
            Constructor of the mini batch k-means clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = MiniBatchKMeans(*args)
        return cls(model)

    @classmethod
    def mean_shift(cls, *args):
        """
            Constructor of the mean shift clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = MeanShift(*args)
        return cls(model)

    @classmethod
    def spectral_clustering(cls, *args):
        """
            Constructor of the spectral clustering.rst algorithm

            :param args: the arguments to be sent to the sci-kit implementation
            :return: returns the Clustering object
        """

        model = SpectralClustering(*args)
        return cls(model)

    @staticmethod
    def is_pyclustering_instance(model):
        """
        Checks if the clustering.rst algorithm belongs to pyclustering

        :param model: the clustering.rst algorithm model
        :return: the truth value (Boolean)
        """
        return any(isinstance(model, i) for i in [xmeans, clarans, rock, optics])

    def fit(self, data=None):
        """
        Performs clustering.rst

        :param data: Data to be fit
        :return: the clustering.rst object
        """
        if self.is_pyclustering_instance(self.model):
            if isinstance(self.model, xmeans):
                data = self.input_preprocess(data)
                self.model._xmeans__pointer_data = data
            elif isinstance(self.model, clarans):
                data = self.input_preprocess(data)
                self.model._clarans__pointer_data = data

            self.model.process()
        else:
            self.model.fit(data)
            return self

    @property
    def _labels_from_pyclusters(self):
        """
        Computes and returns the list of labels indicating the data points and the corresponding cluster ids.

        :return: The list of labels
        """
        clusters = self.model.get_clusters()
        labels = []
        for i in range(0, len(clusters)):
            for j in clusters[i]:
                labels.insert(int(j), i)
        return labels

    def retrieve_cluster(self, df, cluster_no):
        """
        Extracts the cluster at the given index from the input dataframe

        :param df: the dataframe that contains the clusters
        :param cluster_no: the cluster number
        :return: returns the extracted cluster
        """
        if self.is_pyclustering_instance(self.model):
            clusters = self.model.get_clusters()
            mask = []
            for i in range(0, df.shape[0]):
                mask.append(i in clusters[cluster_no])
        else:
            mask = self.model.labels_ == cluster_no  # a boolean mask
        return df[mask]

    @staticmethod
    def get_labels(obj):
        """
        Retrieve the labels of a clustering.rst object

        :param obj: the clustering.rst object
        :return: the resulting labels
        """
        if Clustering.is_pyclustering_instance(obj.model):
            return obj._labels_from_pyclusters
        else:
            return obj.model.labels_

    @staticmethod
    def silhouette_n_clusters(data, k_min, k_max, distance='euclidean'):
        """
        Computes and plot the silhouette score vs number of clusters graph to help selecting the number of clusters visually

        :param data: The data object
        :param k_min: lowerbound of the cluster range
        :param k_max: upperbound of the cluster range
        :param distance: the distance metric, 'euclidean' by default
        :return:
        """
        k_range = range(k_min, k_max)

        k_means_var = [Clustering.kmeans(k).fit(data) for k in k_range]

        silhouette_scores = [obj.silhouette_score(data=data, metric=distance) for obj in k_means_var]

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(k_range, silhouette_scores, 'b*-')
        ax.set_ylim((-1, 1))
        plt.grid(True)
        plt.xlabel('n_clusters')
        plt.ylabel('The silhouette score')
        plt.title('Silhouette score vs. k')
        plt.show()


    @staticmethod
    def elbow_method(data, k_min, k_max, distance='euclidean'):
        """
        Calculates and plots the plot of variance explained - number of clusters
        Implementation reference: https://github.com/sarguido/k-means-clustering.rst

        :param data: The dataset
        :param k_min: lowerbound of the cluster range
        :param k_max: upperbound of the cluster range
        :param distance: the distance metric, 'euclidean' by default
        :return:
        """
        # Determine your k range
        k_range = range(k_min, k_max)

        # Fit the kmeans model for each n_clusters = k
        k_means_var = [Clustering.kmeans(k).fit(data) for k in k_range]

        # Pull out the cluster centers for each model
        centroids = [X.model.cluster_centers_ for X in k_means_var]

        # Calculate the Euclidean distance from
        # each point to each cluster center
        k_euclid = [cdist(data, cent, distance) for cent in centroids]
        dist = [np.min(ke, axis=1) for ke in k_euclid]

        # Total within-cluster sum of squares
        wcss = [sum(d ** 2) for d in dist]

        # The total sum of squares
        tss = sum(pdist(data) ** 2) / data.shape[0]

        # The between-cluster sum of squares
        bss = tss - wcss

        # elbow curve
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot(k_range, bss / tss * 100, 'b*-')
        ax.set_ylim((0, 100))
        plt.grid(True)
        plt.xlabel('n_clusters')
        plt.ylabel('Percentage of variance explained')
        plt.title('Variance Explained vs. k')
        plt.show()

    def adjusted_mutual_info(self, reference_clusters):
        """
        Calculates the adjusted mutual information score w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: returns the value of the adjusted mutual information score
        """
        return adjusted_mutual_info_score(self.get_labels(self), self.get_labels(reference_clusters))

    def adjusted_rand_score(self, reference_clusters):
        """
        Calculates the adjusted rand score w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: returns the value of the adjusted rand score
        """
        return adjusted_rand_score(self.get_labels(self), self.get_labels(reference_clusters))

    def calinski_harabasz(self, data):
        """
        Calculates the Calinski-Harabarsz score for a set of clusters (implicit evaluation).

        :param data: The original dataset that the clusters are generated from
        :return: The resulting Calinski-Harabarsz score
        """
        return calinski_harabaz_score(data, self.get_labels(self))

    def completeness_score(self, reference_clusters):
        """
        Calculates the completeness score w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: the resulting completeness score
        """
        return completeness_score(self.get_labels(self), self.get_labels(reference_clusters))

    def fowlkes_mallows(self, reference_clusters):
        """
        Calculates the Fowlkes-Mallows index (FMI) w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: The resulting Fowlkes-Mallows score.
        """
        return fowlkes_mallows_score(self.get_labels(self), self.get_labels(reference_clusters))

    def homogeneity_score(self, reference_clusters):
        """
        Calculates the homogeneity score w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: The resulting homogeneity score.
        """
        return homogeneity_score(self.get_labels(self), self.get_labels(reference_clusters))

    def mutual_info_score(self, reference_clusters):
        """
        Calculates the MI (mutual information) w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: The resulting MI score.
        """
        return mutual_info_score(self.get_labels(self), self.get_labels(reference_clusters))

    def normalized_mutual_info_score(self, reference_clusters):
        """
        Calculates the normalized mutual information w.r.t. the reference clusters (explicit evaluation)

        :param reference_clusters: Clusters that are to be used as reference
        :return: The resulting normalized mutual information score.
        """
        return normalized_mutual_info_score(self.get_labels(self), self.get_labels(reference_clusters))

    def silhouette_score(self, data,  metric='euclidean', sample_size=None, random_state=None, **kwds):
        """
        Computes the mean Silhouette Coefficient of all samples (implicit evaluation)

        :param data: The data that the clusters are generated from
        :param metric: the pairwise distance metric
        :param sample_size: the size of the sample to use computing the Silhouette Coefficient
        :param random_state: If an integer is given then it fixes its seed otherwise random.
        :param kwds: any further parameters that are passed to the distance function
        :return: the mean Silhouette Coefficient of all samples
        """
        return silhouette_score(data, self.get_labels(self), metric, sample_size, random_state, **kwds)
