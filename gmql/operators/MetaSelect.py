"""
    Selection of metadata based on a logic condition
"""


def meta_select(meta_dataset, predicate):
    """
    Selects rows of the meta dataset dataframe based on a logical predicate
    :param meta_dataset: pandas dataframe of the metadata
    :param predicate: a function (also a lambda) which returns a logical value
    :return: the filtered meta_dataset
    """
    return meta_dataset[meta_dataset.apply(predicate, 1)]
