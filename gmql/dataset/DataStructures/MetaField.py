from ... import get_python_manager


class MetaField:
    
    def __init__(self, name, index=None, meta_condition=None, meNode=None):
        if (meta_condition is not None) and (meNode is not None):
            raise ValueError("you cannot mix conditions and expressions")
        self.index = index
        self.name = name
        self.metaCondition = meta_condition
        pymg = get_python_manager()
        self.exp_build = pymg.getNewExpressionBuilder(self.index)
        
        if not (name.startswith("(") and name.endswith(")")) and meNode is None:
            self.meNode = self.exp_build.getMENode(name)
        else:
            self.meNode = meNode
            
    def getMetaCondition(self):
        return self.metaCondition
    
    def getMetaExpression(self):
        return self.meNode

    """
        PREDICATES
    """
    def __eq__(self, other):
        new_name = '(' + self.name + ' == ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "EQ", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)

    def __ne__(self, other):
        new_name = '(' + self.name + ' != ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "NOTEQ", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)

    def __gt__(self, other):
        new_name = '(' + self.name + ' > ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "GT", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)

    def __ge__(self, other):
        new_name = '(' + self.name + ' >= ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "GTE", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)

    def __lt__(self, other):
        new_name = '(' + self.name + ' < ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "LT", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)

    def __le__(self, other):
        new_name = '(' + self.name + ' <= ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "LTE", str(other))
        return MetaField(name=new_name, meta_condition=predicate, index=self.index)
    """
        CONDITIONS
    """
    def __and__(self, other):
        new_name = '(' + self.name + ' and ' + other.name + ')'
        condition = self.exp_build.createMetaBinaryCondition(self.metaCondition, "AND", other.metaCondition)
        return MetaField(name=new_name, meta_condition=condition, index=self.index)

    def __or__(self, other):
        new_name = '(' + self.name + ' or ' + other.name + ')'
        condition = self.exp_build.createMetaBinaryCondition(self.metaCondition, "OR", other.metaCondition)
        return MetaField(name=new_name, meta_condition=condition, index=self.index)

    def __invert__(self):
        new_name = 'not (' + self.name + ')'
        condition = self.exp_build.createMetaUnaryCondition(self.metaCondition, "NOT")
        return MetaField(name=new_name, meta_condition=condition, index=self.index)

    """
        EXPRESSIONS
    """

    def __get_return_type(self, other):
        if isinstance(other, str):
            other = self.exp_build.getMEType("string", other)
        elif isinstance(other, int):
            other = self.exp_build.getMEType("int", str(other))
        elif isinstance(other, float):
            other = self.exp_build.getMEType("float", str(other))
        else:
            raise ValueError("Expected string, float or integer. {} was found".format(type(other)))
        return other

    def __add__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + self.name + ' + ' + other.name + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "ADD", other.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' + ' + str(other) + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "ADD", other)
        return MetaField(name=new_name, index=self.index, meNode=node)

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + self.name + ' - ' + other.name + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "SUB", other.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' - ' + str(other) + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "SUB", other)
        return MetaField(name=new_name, index=self.index, meNode=node)

    def __rsub__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + other.name + ' - ' + self.name + ')'
            node = self.exp_build.getBinaryMetaExpression(other.meNode, "SUB", self.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + str(other) + ' - ' + self.name + ')'
            node = self.exp_build.getBinaryMetaExpression(other, "SUB", self.meNode)
        return MetaField(name=new_name, index=self.index, meNode=node)

    def __mul__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + self.name + ' * ' + other.name + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "MUL", other.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' * ' + str(other) + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "MUL", other)
        return MetaField(name=new_name, index=self.index, meNode=node)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __truediv__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + self.name + ' / ' + other.name + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "DIV", other.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + self.name + ' / ' + str(other) + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, "DIV", other)
        return MetaField(name=new_name, index=self.index, meNode=node)

    def __rtruediv__(self, other):
        if isinstance(other, MetaField):
            # we are dealing with an other MetaField
            new_name = '(' + other.name + ' / ' + self.name + ')'
            node = self.exp_build.getBinaryMetaExpression(other.meNode, "DIV", self.meNode)
        else:
            other = self.__get_return_type(other)
            new_name = '(' + str(other) + ' / ' + self.name + ')'
            node = self.exp_build.getBinaryMetaExpression(other, "DIV", self.meNode)
        return MetaField(name=new_name, index=self.index, meNode=node)