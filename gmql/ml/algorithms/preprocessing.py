

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