from ... import get_python_manager


class RegField:

    def __init__(self, name, index=None, region_condition=None):
        self.name = name
        self.index = index
        self.region_condition = region_condition
        pymg = get_python_manager()
        self.exp_build = pymg.getNewExpressionBuilder(self.index)

    def getRegionCondition(self):
        return self.region_condition

    """
        PREDICATES
    """

    def __eq__(self, other):
        new_name = '(' + self.name + ' == ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "EQ", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    def __ne__(self, other):
        new_name = '(' + self.name + ' != ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "NOTEQ", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    def __gt__(self, other):
        new_name = '(' + self.name + ' > ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "GT", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    def __ge__(self, other):
        new_name = '(' + self.name + ' >= ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "GTE", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    def __lt__(self, other):
        new_name = '(' + self.name + ' < ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "LT", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    def __le__(self, other):
        new_name = '(' + self.name + ' <= ' + str(other) + ')'
        predicate = self.exp_build.createRegionPredicate(self.name, "LTE", str(other))
        return RegField(name=new_name, region_condition=predicate, index=self.index)

    """
        CONDITIONS
    """

    def __and__(self, other):
        new_name = '(' + self.name + ' and ' + other.name + ')'
        condition = self.exp_build.createRegionBinaryCondition(self.region_condition, "AND", other.region_condition)
        return RegField(name=new_name, region_condition=condition, index=self.index)

    def __or__(self, other):
        new_name = '(' + self.name + ' or ' + other.name + ')'
        condition = self.exp_build.createRegionBinaryCondition(self.region_condition, "OR", other.region_condition)
        return RegField(name=new_name, region_condition=condition, index=self.index)

    def __invert__(self):
        new_name = 'not (' + self.name + ')'
        condition = self.exp_build.createRegionUnaryCondition(self.region_condition, "NOT")
        return RegField(name=new_name, region_condition=condition, index=self.index)

