from .. import GMQLDataset


def materialize(datasets):
    """ Multiple materializations. Enables the user to specify a set of GMQLDataset to be materialized.
    The engine will perform all the materializations at the same time, if an output path is provided,
    while will perform each operation separately if the output_path is not specified.

    :param datasets: it can be a list of GMQLDataset or a dictionary {'output_path' : GMQLDataset}
    :return: a list of GDataframe or a dictionary {'output_path' : GDataframe}
    """
    result = None
    if isinstance(datasets, dict):
        result = dict()
        for output_path in datasets.keys():
            dataset = datasets[output_path]
            if not isinstance(dataset, GMQLDataset.GMQLDataset):
                raise TypeError("The values of the dictionary must be GMQLDataset."
                                " {} was given".format(type(dataset)))
            gframe = dataset.materialize(output_path)
            result[output_path] = gframe
    elif isinstance(datasets, list):
        result = []
        for dataset in datasets:
            if not isinstance(dataset, GMQLDataset.GMQLDataset):
                raise TypeError("The values of the list must be GMQLDataset."
                                " {} was given".format(type(dataset)))
            gframe = dataset.materialize()
            result.append(gframe)
    else:
        raise TypeError("The input must be a dictionary of a list. "
                        "{} was given".format(type(datasets)))
    return result
