from ... import get_python_manager
from .MetaField import MetaField


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

    def is_complex(self):
        return self.name.startswith("(") and self.name.endswith(")")

    def getRegionCondition(self):
        return self.region_condition

    def getRegionExpression(self):
        return self.reNode

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
        new_name = '(' + self.name + operator + str(other) + ')'
        other = self._process_other(other)
        predicate = self.exp_build.createRegionPredicate(self.name, operator, other)
        return RegField(name=new_name, region_condition=predicate, index=self.index)

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
        if isinstance(other, RegField) and other.region_condition is not None:
            new_name = '(' + self.name + operator + other.name + ')'
            condition = self.exp_build.createRegionBinaryCondition(self.region_condition, operator, other.region_condition)
            return RegField(name=new_name, region_condition=condition, index=self.index)
        else:
            raise TypeError("Binary conditions can be done only between RegFields conditions")

    def _unary_condition(self, operator):
        new_name = operator + '(' + self.name + ')'
        condition = self.exp_build.createRegionUnaryCondition(self.region_condition, operator)
        return RegField(name=new_name, region_condition=condition, index=self.index)

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

    def __get_return_type(self, other):
        if isinstance(other, str):
            other_name = other
            other = self.exp_build.getREType("string", other)
        elif isinstance(other, int):
            other_name = str(other)
            other = self.exp_build.getREType("float", str(other))
        elif isinstance(other, float):
            other_name = str(other)
            other = self.exp_build.getREType("float", str(other))
        elif isinstance(other, RegField):
            other_name = other.name
            other = other.reNode
        elif isinstance(other, MetaField):
            if other.index != self.index:
                raise ValueError("You cannot mix fields or attributes of different"
                                 "dataset in a condition or expression")
            if other.reMetaNode is not None:
                other_name = other.name
                other = other.reMetaNode
            else:
                raise ValueError("You cannot use a complex Metadata Expression in a "
                                 "region expression. Only MetaFields are allowed.")
        else:
            raise ValueError("Expected string, float, integer, RegField or MetaField"
                             ". {} was found".format(type(other)))
        return other, other_name

    def _binary_expression(self, other, operation, order="left"):
        other, other_name = self.__get_return_type(other)
        if order == 'left':
            new_name = '(' + self.name + operation + other_name + ')'
            node = self.exp_build.getBinaryRegionExpression(self.reNode, operation, other)
        else:
            new_name = '(' + other_name + operation + self.name + ')'
            node = self.exp_build.getBinaryRegionExpression(other, operation, self.reNode)
        return RegField(name=new_name, index=self.index, reNode=node)

    def _unary_expression(self, operation):
        new_name = operation + "(" + self.name + ")"
        node = self.exp_build.getUnaryRegionExpression(self.reNode, operation)
        return RegField(name=new_name, index=self.index, reNode=node)

    def _process_other(self, other):
        if isinstance(other, MetaField):
            if other.index != self.index:
                raise ValueError("You cannot mix fields or attributes of different"
                                 "dataset in a condition or expression")
            if other.metaAccessor is not None:
                return other.metaAccessor
            else:
                raise ValueError("When mixing RegFields with metadata you can only use "
                                 "MetaFields of a metadata attribute and "
                                 "not complex conditions or expressions on metadata")
        elif isinstance(other, (str, int, float)):
            return str(other)
        else:
            raise TypeError("Other is of unknown type. "
                            "{} provided".format(type(other)))