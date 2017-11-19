class MetadataParser:
    """ Abstract Parser of Metadata files

    """
    def parse_metadata(self, path):
        """ Parse a metadata file into a list of tuple
        [('attr_name', attr_value'), ...]

        :param path: path of the metadata file
        :return: dict
        """
        pass


class GenericMetaParser(MetadataParser):
    def __init__(self, delimiter="\t"):
        self.delimiter = delimiter

    def parse_metadata(self, path):
        fo = open(path)
        lines = fo.read().splitlines()
        return list(map(self._parse_line_metadata, lines))

    def _parse_line_metadata(self, line):
        elems = line.split(self.delimiter)
        return elems[0], elems[1]
