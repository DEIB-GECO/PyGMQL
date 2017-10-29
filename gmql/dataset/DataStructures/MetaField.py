from ... import get_python_manager
from . import RegField, _get_opposite_operator


class MetaField:
    def __init__(self, name, index=None, meta_condition=None, meNode=None, t=None, reMetaNode=None):
        if (meta_condition is not None) and (meNode is not None):
            raise ValueError("you cannot mix conditions and expressions")

        if isinstance(name, str):
            self.name = name
        else:
            raise TypeError("name of the metadata attribute must be a string")
        self.index = index
        pymg = get_python_manager()
        self.exp_build = pymg.getNewExpressionBuilder(self.index)

        if t is not None:
            if isinstance(t, str):
                self.t = t
                self.reMetaNode = self.exp_build.getREMetaAccessor(self.name, pymg.getParseTypeFromString(t))
            else:
                raise TypeError("Type of MetaField must be string")
        else:
            self.t = t
            self.reMetaNode = reMetaNode

        self.metaCondition = meta_condition

        if not (name.startswith("(") and name.endswith(")")) and meNode is None:
            self.meNode = self.exp_build.getMENode(name)
            self.metaAccessor = self.exp_build.getMetaAccessor(name)
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
        return self._predicate(other, "EQ")

    def __ne__(self, other):
        return self._predicate(other, "NOTEQ")

    def __gt__(self, other):
        return self._predicate(other, "GT")

    def __ge__(self, other):
        return self._predicate(other, "GTE")

    def __lt__(self, other):
        return self._predicate(other, "LT")

    def __le__(self, other):
        return self._predicate(other, "LTE")

    def _predicate(self, other, operator):
        if isinstance(other, (int, str, float)):
            new_name = '(' + self.name + operator + str(other) + ')'
            predicate = self.exp_build.createMetaPredicate(self.name, operator, str(other))
            return MetaField(name=new_name, meta_condition=predicate, index=self.index)
        elif isinstance(other, RegField.RegField):
            return other._predicate(self, _get_opposite_operator(operator))
        else:
            raise TypeError("You can create predicates only using primitive types")

    def isin(self, values):
        """ Selects the samples having the metadata attribute between the values provided
        as input

        :param values: a list of elements
        :return a new complex condition
        """
        if not isinstance(values, list):
            raise TypeError("Input should be a string. {} was provided".format(type(values)))
        if not (self.name.startswith("(") and self.name.endswith(")")):
            first = True
            new_condition = None
            for v in values:
                if first:
                    first = False
                    new_condition = self.__eq__(v)
                else:
                    new_condition = new_condition.__or__(self.__eq__(v))
            return new_condition
        else:
            raise SyntaxError("You cannot use 'isin' with a complex condition")

    """
        CONDITIONS
    """

    def __and__(self, other):
        return self._binary_condition(other, "AND")

    def __or__(self, other):
        return self._binary_condition(other, "OR")

    def __invert__(self):
        return self._unary_condition("NOT")

    def _binary_condition(self, other, operator):
        if isinstance(other, MetaField):
            if other.index != self.index:
                raise ValueError("You cannot create conditions using different datasets")
            if other.metaCondition is None or \
                    self.metaCondition is None:
                raise ValueError("One of the two sides of the {} operator"
                                 " is not a condition".format(operator))
            new_name = '(' + self.name + operator + other.name + ')'
            condition = self.exp_build.createMetaBinaryCondition(self.metaCondition, operator, other.metaCondition)
            return MetaField(name=new_name, meta_condition=condition, index=self.index)
        else:
            raise TypeError("Binary conditions can be done only between MetaFields")

    def _unary_condition(self, operator):
        if self.metaCondition is None:
            raise ValueError("The inner part of the {} operator is not"
                             " a condition".format(operator))
        new_name = operator + '(' + self.name + ')'
        condition = self.exp_build.createMetaUnaryCondition(self.metaCondition, operator)
        return MetaField(name=new_name, meta_condition=condition, index=self.index)

    """
        EXPRESSIONS
    """

    def __add__(self, other):
        return self._binary_expression(other, "ADD")

    def __radd__(self, other):
        return self.__add__(other)

    def __sub__(self, other):
        return self._binary_expression(other, "SUB", order="left")

    def __rsub__(self, other):
        return self._binary_expression(other, "SUB", order="right")

    def __mul__(self, other):
        return self._binary_expression(other, "MUL")

    def __rmul__(self, other):
        return self.__mul__(other)

    def __truediv__(self, other):
        return self._binary_expression(other, "DIV", order="left")

    def __rtruediv__(self, other):
        return self._binary_expression(other, "DIV", order="right")

    def __neg__(self):
        return self._unary_expression("NEG")

    def _binary_expression(self, other, operation, order="left"):
        if self.meNode is None:
            raise ValueError("Cannot mix expressions and conditions")
        if isinstance(other, RegField.RegField):
            return other._binary_expression(self, operation, "right" if order == "left" else "left")
        other, other_name, other_reNode = self.__get_return_type(other)
        renode = None
        if order == 'left':
            new_name = '(' + self.name + operation + other_name + ')'
            node = self.exp_build.getBinaryMetaExpression(self.meNode, operation, other)
            if other_reNode is not None:
                renode = self.exp_build.getBinaryRegionExpression(self.reMetaNode, operation, other_reNode)
        else:
            new_name = '(' + other_name + operation + self.name + ')'
            node = self.exp_build.getBinaryMetaExpression(other, operation, self.meNode)
            if other_reNode is not None:
                renode = self.exp_build.getBinaryRegionExpression(other_reNode, operation, self.reMetaNode)
        return MetaField(name=new_name, index=self.index, meNode=node, reMetaNode=renode)

    def _unary_expression(self, operation):
        if self.meNode is None:
            raise ValueError("Cannot mix expressions and conditions")
        new_name = operation + "(" + self.name + ")"
        node = self.exp_build.getUnaryMetaExpression(self.meNode, operation)
        renode = self.exp_build.getUnaryRegionExpression(self.reMetaNode, operation)
        return MetaField(name=new_name, index=self.index, meNode=node, reMetaNode=renode)

    def __get_return_type(self, other):
        if isinstance(other, str):
            other_reNode = self.exp_build.getREType("string", other)
            other_name = other
            other = self.exp_build.getMEType("string", other)
        elif isinstance(other, int):
            other_reNode = self.exp_build.getREType("int", str(other))
            other_name = str(other)
            other = self.exp_build.getMEType("int", str(other))
        elif isinstance(other, float):
            other_reNode = self.exp_build.getREType("float", str(other))
            other_name = str(other)
            other = self.exp_build.getMEType("float", str(other))
        elif isinstance(other, MetaField):
            other_name = other.name
            if other.index != self.index:
                raise ValueError("You cannot mix fields or attributes of different"
                                 "dataset in a condition or expression")
            if other.meNode is None:
                raise ValueError("You cannot mix conditions and expressions")
            other_reNode = other.reMetaNode
            other = other.meNode
        else:
            raise ValueError("Expected string, float, integer or MetaField. {} was found".format(type(other)))
        return other, other_name, other_reNode
