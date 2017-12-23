reg_fixed_fileds = ['chr', 'start', 'stop', 'strand']

chr_aliases = ['chr', 'chrom', 'chromosome']
start_aliases = ['start', 'left']
stop_aliases = ['stop', 'right', 'end']
strand_aliases = ['str', 'strand']
id_sample_aliases = ['id_sample', 'sample_id', 'sample']

operator_correspondences = {
    'LT': 'GT',
    'GT': 'LT',
    'LTE': 'GTE',
    'GTE': 'LTE',
    'EQ': 'EQ',
    'NOTEQ': 'NOTEQ'
}


def _get_opposite_operator(operator):
    return operator_correspondences[operator]
