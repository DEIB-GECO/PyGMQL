import os

user_path = os.path.expanduser("~")
USER_FOLDER_NAME = ".pygmql"


def initialize_user_folder():
    if not os.path.isdir(get_user_dir()):
        os.makedirs(get_user_dir())


def get_user_dir():
    return os.path.join(user_path, USER_FOLDER_NAME)