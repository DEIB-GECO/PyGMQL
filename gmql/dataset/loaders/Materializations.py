
import os
from glob import glob
from ...managers import get_python_manager, get_remote_manager, get_source_table
from . import MetaLoaderFile, RegLoaderFile, MemoryLoader, Loader
from ...FileManagment.TempFileManager import get_unique_identifier, get_new_dataset_tmp_folder


def materialize(datasets):
    """ Multiple materializations. Enables the user to specify a set of GMQLDataset to be materialized.
    The engine will perform all the materializations at the same time, if an output path is provided,
    while will perform each operation separately if the output_path is not specified.

    :param datasets: it can be a list of GMQLDataset or a dictionary {'output_path' : GMQLDataset}
    :return: a list of GDataframe or a dictionary {'output_path' : GDataframe}
    """
    from .. import GMQLDataset
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


def materialize_local(id, output_path=None):
    from .. import GDataframe
    pmg = get_python_manager()

    if output_path is not None:
        # check that the folder does not exists
        if os.path.isdir(output_path):
            raise ValueError("Folder {} already exists!".format(output_path))

        pmg.materialize(id, output_path)
        pmg.execute()

        # taking in memory the data structure
        real_path = Loader.preprocess_path(output_path)
        # real_path = os.path.join(output_path, 'exp')

        remove_side_effects(real_path)
        # metadata
        meta = MetaLoaderFile.load_meta_from_path(real_path)
        # region data
        regs = RegLoaderFile.load_reg_from_path(real_path)
    else:
        # We load the structure directly from the memory
        collected = pmg.collect(id)
        regs = MemoryLoader.load_regions(collected)
        meta = MemoryLoader.load_metadata(collected)

    result = GDataframe.GDataframe(regs=regs, meta=meta)
    return result


def remove_side_effects(path):
    for f in glob(os.path.join(path, ".*")) + glob(os.path.join(path, "_*")):
        os.remove(f)


def materialize_remote(id, output_name=None, download_path=None, all_load=True):
    pmg = get_python_manager()
    if not isinstance(output_name, str):
        output_name = get_unique_identifier()
    pmg.materialize(id, output_name)
    remote_manager = get_remote_manager()
    if (download_path is None) and all_load:
        download_path = get_new_dataset_tmp_folder()
    result = remote_manager.execute_remote_all(output_path=download_path)
    # pmg.getServer().clearMaterializationList()
    if result.shape[0] == 1:  # TODO: change this!!!
        dataset_name = result.iloc[0].dataset
        st = get_source_table()
        st.add_source(remote=dataset_name, delete_remote=True)
        dataset_path = result.iloc[0].path
        return Loader.load_from_path(local_path=dataset_path, all_load=all_load)
