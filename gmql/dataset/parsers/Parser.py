

class Parser:

    def parse_line_reg(self, id_record, line):
        """
        Parses a line of region data
        :param id_record: the id given by the GMQL System to the record
        :param line: the string representing the line of the file
        :return: the parsed line
        """
        pass

    def parse_line_meta(self, id_record, line):
        """
        Parses a line of metadata
        :param id_record: the id given by the GMQL System to the record
        :param line: the string representing the line of the file
        :return: the parsed line
        """
        pass

    def get_attributes(self):
        pass

    def get_ordered_attributes(self):
        pass

    def get_types(self):
        pass

    def get_ordered_types(self):
        pass

    def get_name_type_dict(self):
        pass

    def get_parser_name(self):
        pass

    def get_gmql_parser(self):
        pass

    def get_parser_type(self):
        pass


def parse_strand(strand):
    if strand in ['+', '-', '*']:
        return strand
    else:
        return '*'

