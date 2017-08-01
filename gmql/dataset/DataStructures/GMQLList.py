class GMQLList:
    def __init__(self, element_list):
        if isinstance(element_list, list):
            self.element_list = element_list
        else:
            raise TypeError("List was expected. Got {}".format(type(element_list)))

        def __getitem__(self, item):
            if isinstance(item, int):
                return self.element_list[item]
            else:
                raise TypeError("Indices must be integers. Got {}".format(type(item)))

        def __str__(self):
            return self.element_list.__str__()

        def __eq__(self, other):
            return other in self.element_list
