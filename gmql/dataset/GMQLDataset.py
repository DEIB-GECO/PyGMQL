from gmql import sc

class GMQLDataset:

    def load_from_path(self, path, parser):
        rdd = sc.textFile(path).map(parser.parse_line)
        return rdd

    def meta_select(self, predicate):
