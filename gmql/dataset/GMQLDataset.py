import pyspark
import pandas as pd
from .. import conf

class GMQLDataset:
    dataset = None

    def __init__(self, dataset=None, parser=None):
        self.dataset = dataset
        self.parser = parser

    def load_from_path(self, path):
        sc = pyspark.SparkContext.getOrCreate(conf=conf)
        rdd = sc.textFile(path+'/*').map(self.parser.parse_line)
        return GMQLDataset(dataset=rdd)

    def meta_select(self, predicate):
        rdd = self.dataset.filter(predicate)
        return GMQLDataset(dataset=rdd)

    def get_snippet_dataframe(self, n):
        snippet_rdd = self.dataset.take(n)
        df = pd.DataFrame.from_dict(snippet_rdd)
        return df


