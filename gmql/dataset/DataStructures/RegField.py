from ... import get_python_manager


class RegField:

    def __init__(self, name, index=None, region_condition=None, reNode=None):
        if (region_condition is not None) and (reNode is not None):
            raise ValueError("you cannot mix conditions and expressions")
        self.name = name
        self.index = index
        self.region_condition = region_condition
        pymg = get_python_manager()
        self.exp_build = pymg.getNewExpressionBuilder(self.index)
        # check that the name is not already complex
        if not (name.startswith("(") and name.endswith(")")) and reNode is None:
            self.reNode = self.exp_build.getRENode(name)
        else:
            self.reNode = reNode

    def getRegionCondition(self):
        return self.region_condition

    def getRegionExpression(self):
        return self.reNode

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

    """
        EXPRESSIONS
    """

    def __get_return_type(self, other):
        if isinstance(other, str):
            other = self.exp_build.getREType("string", other)
        elif isinstance(other, int):
            other = self.exp_build.getREType("int", str(other))
        elif isinstance(other, float):
            other = self.exp_build.getREType("float", str(other))
        else:
            raise ValueError("Expected string, float or integer. {} was found".format(type(other)))
        return other

    def __add__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + self.name + ' + ' + other.name + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "ADD", other.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' + ' + str(other) + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "ADD", other)
        return RegField(name=new_name, index=self.index, reNode=node)

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + self.name + ' - ' + other.name + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "SUB", other.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' - ' + str(other) + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "SUB", other)
        return RegField(name=new_name, index=self.index, reNode=node)

    def __rsub__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + other.name + ' - ' + self.name + ')'
            node = self.exp_build.getBinaryRegionExpression(other.reNode, "SUB", self.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + str(other) + ' - ' + self.name + ')'
            node = self.exp_build.getBinaryRegionExpression(other, "SUB", self.reNode)
        return RegField(name=new_name, index=self.index, reNode=node)

    def __mul__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + self.name + ' * ' + other.name + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "MUL", other.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' * ' + str(other) + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "MUL", other)
        return RegField(name=new_name, index=self.index, reNode=node)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __truediv__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + self.name + ' / ' + other.name + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "DIV", other.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' / ' + str(other) + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, "DIV", other)
        return RegField(name=new_name, index=self.index, reNode=node)

    def __rtruediv__(self, other):
        if isinstance(other, RegField):
            # we are dealing with an other RegField
            new_name = '(' + other.name + ' / ' + self.name + ')'
            node = self.exp_build.getBinaryRegionExpression(other.reNode, "DIV", self.reNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + str(other) + ' / ' + self.name + ')'
            node = self.exp_build.getBinaryRegionExpression(other, "DIV", self.reNode)
        return RegField(name=new_name, index=self.index, reNode=node)
