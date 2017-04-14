class RegRecord:
    """
    Representation of a single region record
    """
    def __init__(self, chr, start, stop, strand):
        """
        There are four mandatory attributes
        :param chr: chromosome
        :param start: start of the region
        :param stop: stop of the region
        :param strand: can be '+', '-' or '*'
        
        The other attributes are domain dependent and are added dynamically
        through the __setattr__ method.
        """
        self.chr = chr
        self.start = start
        self.stop = stop
        self.strand = strand

    def to_dictionary(self):
        d = {
            'chr': self.chr,
            'start': self.start,
            'stop': self.stop,
            'strand': self.strand
        }
        attrs = dir(self)
        # take only the ones that are not mandatory
        attrs = [a for a in attrs if not a.startswith("__") and
                 a not in ['chr', 'start', 'stop', 'strand','to_dictionary']]
        for a in attrs:
            d[a] = self.__getattribute__(a)

        return d
