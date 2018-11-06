from .loaders.Loader import load_from_path
from ..FileManagment import get_resources_dir
import os


def get_example_dataset(name="Example_Dataset_1", load=False):
    data_path = os.path.join(get_resources_dir(), "example_datasets", name)
    return load_from_path(data_path, all_load=load)
