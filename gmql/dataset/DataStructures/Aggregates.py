class Aggregate(object):
    def __init__(self, name, argument=None):
        self.argument = argument
        self.name = name

    def get_aggregate_name(self):
        return self.name

    def get_argument(self):
        return self.argument


class COUNT(Aggregate):
    def __init__(self):
        super(COUNT, self).__init__("SUM", "")


class SUM(Aggregate):
    def __init__(self, argument):
        super(SUM, self).__init__("SUM", argument)


class MIN(Aggregate):
    def __init__(self, argument):
        super(MIN, self).__init__("MIN", argument)


class MAX(Aggregate):
    def __init__(self, argument):
        super(MAX, self).__init__("MAX", argument)


class AVG(Aggregate):
    def __init__(self, argument):
        super(AVG, self).__init__("AVG", argument)


class BAG(Aggregate):
    def __init__(self, argument):
        super(BAG, self).__init__("BAG", argument)


class STD(Aggregate):
    def __init__(self, argument):
        super(STD, self).__init__("STD", argument)


class MEDIAN(Aggregate):
    def __init__(self, argument):
        super(MEDIAN, self).__init__("MEDIAN", argument)


class Q1(Aggregate):
    def __init__(self, argument):
        super(Q1, self).__init__("Q1", argument)


class Q2(Aggregate):
    def __init__(self, argument):
        super(Q2, self).__init__("Q2", argument)


class Q3(Aggregate):
    def __init__(self, argument):
        super(Q3, self).__init__("Q3", argument)