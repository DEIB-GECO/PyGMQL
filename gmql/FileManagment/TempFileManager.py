from pkg_resources import resource_filename
import os, shutil, uuid


resource_folder = resource_filename("gmql", "resources")
tmp_folder_name = os.path.join(resource_folder, "tmp")
tmp_folder_spark = os.path.join(tmp_folder_name, "spark")
tmp_folder_datasets = os.path.join(tmp_folder_name, "datasets")

tmp_folders = [tmp_folder_name, tmp_folder_spark, tmp_folder_datasets]


def initialize_tmp_folders():
    for tf in tmp_folders:
        if os.path.isdir(tf):
            shutil.rmtree(tf)
        os.mkdir(tf)


def get_unique_identifier():
    return str(uuid.uuid4())


def get_new_dataset_tmp_folder():
    name = get_unique_identifier()
    return os.path.join(tmp_folder_datasets, name)


def delete_tmp_dataset(path):
    if os.path.isdir(path):
        shutil.rmtree(path)


def flush_tmp_folder(folder):
    if os.path.isdir(folder):
        shutil.rmtree(folder)


def flush_everything():
    flush_tmp_folder(tmp_folder_name)

