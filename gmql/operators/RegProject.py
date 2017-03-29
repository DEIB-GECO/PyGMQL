"""
    PROJECTION ON REGION DATA
    Creates, from an existing dataset, a new dataset with all the samples in the input one,
    but keeping for each sample in the input dataset only those region attributes expressed 
    in the operator parameter list. It enables also to create new region fields based on the values
    of the others
"""


def project_sample(reg,field_list, new_field_dict):
    # reg is a dictionary of (fields, value)
    if new_field_dict is not None:
        for k in new_field_dict.keys():
            fun = new_field_dict[k]
            reg[k] = fun(reg)
    return {k: v for k, v in reg.items() if k in field_list}


def reg_project(reg_dataset, field_list, new_field_dict):
    if new_field_dict is not None:
        field_list.extend(new_field_dict.keys())
    regs = reg_dataset.map(lambda x: project_sample(x, field_list, new_field_dict))
    return regs
