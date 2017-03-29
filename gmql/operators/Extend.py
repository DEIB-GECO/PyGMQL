"""
    EXTEND
    For each sample in an input dataset, the EXTEND operator builds new metadata attributes,
    assigns their values as the result of arithmetic and/or aggregate functions calculated on
    sample region attributes, and adds them to the existing metadata attribute-value pairs of
    the sample. Sample number and their genomic regions, with their attributes and values,
    remain unchanged in the output dataset.  
"""


def apply_aggregations(tuple, aggregate_fun):
    res = {}
    res['id_sample'] = tuple[0]
    for k in aggregate_fun.keys():
        r = aggregate_fun[k](tuple[1])  # apply the function to the dictionary
        res[k] = r
    return res


def to_dict_of_lists(list_of_dict):
    keys = list_of_dict[0].keys()   # exploit the fact that all the dicts have the same keys
    res = {}
    for k in keys:
        l = []
        for d in list_of_dict:
            l.append(d[k])
        res[k] = l
    return res


def extend(reg_dataset, aggregate_fun):
    regs = reg_dataset.map(lambda x : (x[0], [x[1]]))           # translate to list
    regs = regs.reduceByKey(lambda x,y : x + y)                 # aggregate the lists of elements (dicts)
    regs = regs.map(lambda x : (x[0], to_dict_of_lists(x[1])))  # convert to dictionary of lists

    # apply every function
    regs = regs.map(lambda x : apply_aggregations(x, aggregate_fun))
    # each row is {'id_sample': id, 'new_attr': value, ...}

    # collecting
    regs = regs.collect()

    # TODO: transformation into metadata pandas dataframe




