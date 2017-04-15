from ... import get_python_manager


class MetaField:
    def __init__(self, name, meta_condition=None):
        self.name = name
        self.metaCondition = meta_condition

        pymg = get_python_manager()
        self.exp_build = pymg.getNewExpressionBuilder()

    def getMetaCondition(self):
        return self.metaCondition

    """
        PREDICATES
    """
    def __eq__(self, other):
        new_name = '(' + self.name + ' == ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "EQ", str(other))
        return MetaField(name=new_name, meta_condition=predicate)

    def __ne__(self, other):
        new_name = '(' + self.name + ' != ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "NOTEQ", str(other))
        return MetaField(name=new_name, meta_condition=predicate)

    def __gt__(self, other):
        new_name = '(' + self.name + ' > ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "GT", str(other))
        return MetaField(name=new_name, meta_condition=predicate)

    def __ge__(self, other):
        new_name = '(' + self.name + ' >= ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "GTE", str(other))
        return MetaField(name=new_name, meta_condition=predicate)

    def __lt__(self, other):
        new_name = '(' + self.name + ' < ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "LT", str(other))
        return MetaField(name=new_name, meta_condition=predicate)

    def __le__(self, other):
        new_name = '(' + self.name + ' <= ' + str(other) + ')'
        predicate = self.exp_build.createMetaPredicate(self.name, "LTE", str(other))
        return MetaField(name=new_name, meta_condition=predicate)
    """
        CONDITIONS
    """
    def __and__(self, other):
        new_name = '(' + self.name + ' and ' + other.name + ')'
        condition = self.exp_build.createMetaBinaryCondition(self.metaCondition, "AND", other.metaCondition)
        return MetaField(name=new_name, meta_condition=condition)

    def __or__(self, other):
        new_name = '(' + self.name + ' or ' + other.name + ')'
        condition = self.exp_build.createMetaBinaryCondition(self.metaCondition, "OR", other.metaCondition)
        return MetaField(name=new_name, meta_condition=condition)

    def __invert__(self):
        new_name = 'not (' + self.name + ')'
        condition = self.exp_build.createMetaUnaryCondition(self.metaCondition, "NOT")
        return MetaField(name=new_name, meta_condition=condition)
