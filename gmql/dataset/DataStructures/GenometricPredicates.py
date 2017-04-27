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
    def __init__(self, limit):
        super(DLE, self).__init__(condition_name="DLE", argument=limit)


class DGE(GenometricCondition):
    def __init__(self, limit):
        super(DGE, self).__init__(condition_name="DGE", argument=limit)


class MD(GenometricCondition):
    def __init__(self, number):
        super(MD, self).__init__(condition_name="MD", argument=number)


class UP(GenometricCondition):
    def __init__(self):
        super(UP, self).__init__(condition_name="UP")


class DOWN(GenometricCondition):
    def __init__(self):
        super(DOWN, self).__init__(condition_name="DOWN")
