from pkg_resources import resource_filename
from .loaders.Loader import load_from_path
import os


def get_example_dataset(name="Example_Dataset_1", load=False):
    data_path = resource_filename("gmql", os.path.join("resources", "example_datasets", name))
    return load_from_path(data_path, all_load=load)