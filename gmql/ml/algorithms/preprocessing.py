from fancyimpute import KNN, SimpleFill, IterativeSVD
import pandas as pd

class Preprocessing:
    """
    Contains the functionalities for data standardization, feature selection and missing value imputation
    """
    def __init__(self):
        return

    @staticmethod
    def to_zero_mean(df):
        """
        Standardizes the data by shifting the mean value to zero
        :param df: the input dataframe
        :return: the resulting dataframe
        """
        df_norm = (df - df.mean())
        return df_norm

    @staticmethod
    def to_unit_variance(df):
        """
        Makes the variance of each gene equal to one
        :param df: the dataframe
        :return: the resulting dataframe
        """
        df_norm = df / (df.max() - df.min())
        return df_norm

    @staticmethod
    def prune_by_missing_percent(df, percentage=0.4):
        """
        The method to remove the attributes (genes) with more than a percentage of missing values
        :param df: the dataframe containing the attributes to be pruned
        :param percentage: the percentage threshold (0.4 by default)
        :return: the pruned dataframe
        """
        mask = (df.isnull().sum() / df.shape[0]).map(lambda x: True if x < percentage else False)
        pruned_df = df[df.columns[mask.values]]
        return pruned_df

    @staticmethod
    def impute_using_statistics(df, method='min'):
        """
        Imputes the missing values by the selected statistical property of each column
        :param df: The input dataframe that contains missing values
        :param method: The imputation method (min by default)
            "zero": fill missing entries with zeros
            "mean": fill with column means
            "median" : fill with column medians
            "min": fill with min value per column
            "random": fill with gaussian noise according to mean/std of column
        :return: the imputed dataframe
        """
        sf = SimpleFill(method)
        imputed_matrix = sf.complete(df.values)
        imputed_df = pd.DataFrame(imputed_matrix, df.index, df.columns)
        return imputed_df

    @staticmethod
    def impute_knn(df, k=3):
        """
        Nearest neighbour imputations which weights samples using the mean squared difference on features for which two rows both have observed data.
        :param df: The input dataframe that contains missing values
        :param k: The number of neighbours
        :return: the imputed dataframe
        """
        imputed_matrix = KNN(k=k).complete(df.values)
        imputed_df = pd.DataFrame(imputed_matrix, df.index, df.columns)
        return imputed_df

    @staticmethod
    def impute_svd(df, rank=10, convergence_threshold=0.00001, max_iters=200):
        """
        Imputes the missing values by using SVD decomposition
        :param df:The input dataframe that contains missing values
        :param rank: Rank value of the truncated SVD
        :param convergence_threshold: The threshold to stop the iterations
        :param max_iters: Max number of iterations
        :return: the imputed dataframe
        """
        imputed_matrix = IterativeSVD(rank,convergence_threshold, max_iters).complete(df.values)
        imputed_df = pd.DataFrame(imputed_matrix, df.index, df.columns)
        return imputed_df
