from ... import get_python_manager


class GenometricCondition(object):
    def __init__(self, condition_name, argument=None):
        self.condition_name = condition_name
        self.argument = argument
        self.gen_condition = get_python_manager()   \
                            .getOperatorManager() \
                            .getGenometricCondition(self.condition_name, str(self.argument))

    def get_condition_name(self):
        return self.condition_name

    def get_argument(self):
        return self.argument

    def get_gen_condition(self):
        return self.gen_condition


class DLE(GenometricCondition):
    """ Denotes the less-equal distance clause, which selects all the regions of
     the experiment such that their distance from the anchor region is less than, 
     or equal to, N bases. There are two special less-equal distances clauses: 
     DLE(-1) searches for regions of the experiment which overlap with the anchor 
     region (regardless the extent of the overlap), while DLE(0) searched for experiment 
     regions adjacent to, or overlapping, the anchor region
     """
    def __init__(self, limit):
        super(DLE, self).__init__(condition_name="DLE", argument=limit)


class DGE(GenometricCondition):
    """ Greater-equal distance clause, which selects all the regions of the experiment 
    such that their distance from the anchor region is greater than, or equal to, N bases"""
    def __init__(self, limit):
        super(DGE, self).__init__(condition_name="DGE", argument=limit)


class DL(GenometricCondition):
    """ Less than distance clause, which selects all the regions of the experiment
    such that their distance from the anchor region is less than N bases"""
    def __init__(self, limit):
        super(DL, self).__init__(condition_name="DL", argument=limit)


class DG(GenometricCondition):
    """ Greater than distance clause, which selects all the regions of the experiment
    such that their distance from the anchor region is greater than N bases"""
    def __init__(self, limit):
        super(DG, self).__init__(condition_name="DG", argument=limit)


class MD(GenometricCondition):
    """ Denotes the minimum distance clause, which selects the first K regions of an 
    experiment sample at minimal distance from an anchor region of an anchor dataset 
    sample. In case of ties (i.e., regions at the same distance from the anchor region), 
    all tied experiment regions are kept in the result, even if they would exceed 
    the limit of K
    """
    def __init__(self, number):
        super(MD, self).__init__(condition_name="MD", argument=number)


class UP(GenometricCondition):
    """ Upstream."""
    def __init__(self):
        super(UP, self).__init__(condition_name="UP")


class DOWN(GenometricCondition):
    """ Downstream."""
    def __init__(self):
        super(DOWN, self).__init__(condition_name="DOWN")
