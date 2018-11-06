import os
from pkg_resources import resource_filename


user_path = os.path.expanduser("~")
USER_FOLDER_NAME = ".pygmql"

user_folder = os.path.join(user_path, USER_FOLDER_NAME)
resource_folder = resource_filename("gmql", "resources")


def initialize_user_folder():
    if not os.path.isdir(get_user_dir()):
        os.makedirs(get_user_dir())


def get_user_dir():
    return user_folder


def get_resources_dir():
    return resource_folder
