class GDataframe:
    """ Class holding the result of a materialization of a GMQLDataset.
    It is composed by two data structures:
    
        - A table with the *region* data
        - A table with the *metadata* corresponding to the regions
    """
    def __init__(self, regs, meta):
        self.regs = regs
        self.meta = meta

    def toGS(self):
        """ Translates the GDataframe to the Genomic Space data structure
        """
        raise NotImplementedError("Not yet implemented")


