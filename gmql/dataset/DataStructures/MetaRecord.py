"""
    Representation of a Metadata record
"""
class MetaRecord:
    def __init__(self):
        pass

    def from_dict(self, d):
        for k in d.keys():
            self.__setattr__(k, d[k])
