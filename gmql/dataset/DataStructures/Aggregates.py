class Aggregate(object):
    def __init__(self, name, argument=None):
        self.argument = argument
        self.name = name

    def get_aggregate_name(self):
        return self.name

    def get_argument(self):
        return self.argument

    def is_nullary(self):
        return self.argument is None

    def is_unary(self):
        return self.argument is not None


class COUNT(Aggregate):
    """ Counts the number of regions in the group. It is automatically computed by the
    :func:`~gmql.dataset.GMQLDataset.GMQLDataset.map` operation
    """
    def __init__(self):
        super(COUNT, self).__init__("COUNT")


class COUNTSAMP(Aggregate):
    """ Counts the number of samples in the group.
    """
    def __init__(self):
        super(COUNTSAMP, self).__init__("COUNTSAMP")


class SUM(Aggregate):
    """ Computes the sum of the values of the specified attribute
    """
    def __init__(self, argument):
        super(SUM, self).__init__("SUM", argument)


class MIN(Aggregate):
    """ Gets the minimum value in the aggregation group for the specified attribute
    """
    def __init__(self, argument):
        super(MIN, self).__init__("MIN", argument)


class MAX(Aggregate):
    """ Gets the maximum value in the aggregation group for the specified attribute
    """
    def __init__(self, argument):
        super(MAX, self).__init__("MAX", argument)


class AVG(Aggregate):
    """ Gets the average value in the aggregation group for the specified attribute
    """
    def __init__(self, argument):
        super(AVG, self).__init__("AVG", argument)


class BAG(Aggregate):
    """ Creates space-separated string of attribute values for the specified attribute.
    It is applicable to attributes of any type.
    """
    def __init__(self, argument):
        super(BAG, self).__init__("BAG", argument)


class BAGD(Aggregate):
    """ Creates space-separated string of DISTINCT attribute values for the specified attribute.
    It is applicable to attributes of any type.
    """
    def __init__(self, argument):
        super(BAGD, self).__init__("BAGD", argument)


class STD(Aggregate):
    """ Gets the standard deviation of the aggregation group for the specified attribute
    """
    def __init__(self, argument):
        super(STD, self).__init__("STD", argument)


class MEDIAN(Aggregate):
    """ Gets the median value of the aggregation group for the specified attribute
    """
    def __init__(self, argument):
        super(MEDIAN, self).__init__("MEDIAN", argument)


class Q1(Aggregate):
    """ Gets the first quartile for the specified attribute
    """
    def __init__(self, argument):
        super(Q1, self).__init__("Q1", argument)


class Q2(Aggregate):
    """ Gets the second quartile for the specified attribute
    """
    def __init__(self, argument):
        super(Q2, self).__init__("Q2", argument)


class Q3(Aggregate):
    """ Gets the third quartile for the specified attribute
    """
    def __init__(self, argument):
        super(Q3, self).__init__("Q3", argument)