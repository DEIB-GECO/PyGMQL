"""
Selection of regions based on logical predicate
"""


def reg_select(reg_dataset, predicate):
    s = reg_dataset.filter(lambda row: predicate(row[1]))
    return s