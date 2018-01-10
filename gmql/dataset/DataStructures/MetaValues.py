class MetaValues:
    """ Class holding the multiple values of a cell of a GDataframe
    """
    def __init__(self, values):
        if isinstance(values, set):
            self.values = values
        else:
            raise TypeError("set was expected. Got {}".format(type(values)))

    def __str__(self):
        return ", ".join(sorted(self.values))

    def __eq__(self, other):
        if isinstance(other, MetaValues):
            return len(self.values.intersection(other.values)) > 0
        else:
            return other in self.values

    # def __gt__(self, other):
    #     if isinstance(other, MetaValues):
