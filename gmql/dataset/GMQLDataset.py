from gmql import sc

class GMQLDataset:
    dataset = None

    def __init__(self, dataset=None):
        self.dataset = dataset

    def load_from_path(self, path, parser):
        rdd = sc.textFile(path).map(parser.parse_line)
        return GMQLDataset(dataset=rdd)

    def meta_select(self, predicate):
        rdd = self.dataset.filter(predicate)
