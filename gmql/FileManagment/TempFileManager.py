from pkg_resources import resource_filename
import os, shutil, time


def get_current_time():
    return time.strftime("%Y%m%d_%H%M%S")


resource_folder = resource_filename("gmql", "resources")
tmp_folder_name = os.path.join(resource_folder, "tmp")
tmp_folder_instance = os.path.join(tmp_folder_name, "Instance_" + get_current_time())
tmp_folder_spark = os.path.join(tmp_folder_instance, "spark")
tmp_folder_datasets = os.path.join(tmp_folder_instance, "datasets")

tmp_folders = [tmp_folder_name, tmp_folder_instance, tmp_folder_spark, tmp_folder_datasets]


def initialize_tmp_folders():
    for tf in tmp_folders:
        if not os.path.isdir(tf):
            os.mkdir(tf)
    result = {
        'tmp': tmp_folder_name,
        'instance': tmp_folder_instance,
        'spark': tmp_folder_spark,
        'datasets': tmp_folder_datasets
    }
    return result


def get_unique_identifier():
    return "id_" + get_current_time()


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
    flush_tmp_folder(tmp_folder_instance)
    # check if the top folder is empty or not
    if not os.listdir(tmp_folder_name):
        flush_tmp_folder(tmp_folder_name)

