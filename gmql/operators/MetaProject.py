"""
    Projection of metadata:
        Creates, from an existing dataset, a new dataset with all the samples in the input one,
        but keeping for each sample in the input dataset only those metadata attributes expressed
        in the operator parameter list.
"""


def apply_function(fun, row):
    result = fun(row)
    if type(result) is not list:
        result = [result]
    return result


def meta_project(meta_dataset, attr_list, new_attr_list):
    meta = meta_dataset[attr_list]
    if new_attr_list is not None:
        for new_attr in new_attr_list.keys():
            meta[new_attr] = meta.apply(lambda row : apply_function(new_attr_list[new_attr], row), 1)

    return meta
