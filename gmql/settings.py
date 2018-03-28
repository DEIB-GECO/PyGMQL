from pkg_resources import resource_filename
from .FileManagment.SessionFileManager import initialize_user_folder
from .FileManagment import TempFileManager
import os


__version__ = None
__progress_bar = True
__metadata_profiling = True
__remote_address = None
__mode = "local"
__folders = None


def set_mode(how):
    """ Sets the behavior of the API

    :param how: if 'remote' all the execution is performed on the remote server; if 'local' all
           it is executed locally. Default = 'local'
    :return: None
    """
    global __mode
    if how == "local":
        __mode = how
    elif how == "remote":
        __mode = how
    else:
        raise ValueError("how must be 'local' or 'remote'")


def get_mode():
    global __mode
    return __mode


def get_version():
    version_file_name = resource_filename("gmql", os.path.join("resources", "version"))
    with open(version_file_name, "r") as f_ver:
        version = f_ver.read().strip()
    return version


def set_progress(how):
    """ Enables or disables the progress bars for the loading, writing and downloading
    of datasets

    :param how: True if you want the progress bar, False otherwise
    :return: None

    Example::

        import gmql as gl

        gl.set_progress(True)   # abilitates progress bars
        # ....do something...
        gl.set_progress(False)  # removes progress bars
        # ....do something...
    """
    global __progress_bar
    if isinstance(how, bool):
        __progress_bar = how
    else:
        raise ValueError(
            "how must be a boolean. {} was found".format(type(how)))


def is_progress_enabled():
    global __progress_bar
    return __progress_bar


def set_meta_profiling(how):
    """ Enables or disables the profiling of metadata at the loading of a GMQLDataset

    :param how: True if you want to analyze the metadata when a GMQLDataset is created
                by a load_from_*. False otherwise. (Default=True)
    :return: None
    """
    global __metadata_profiling
    if isinstance(how, bool):
        __metadata_profiling = how
    else:
        raise TypeError("how must be boolean. {} was provided".format(type(how)))


def is_metaprofiling_enabled():
    global __metadata_profiling
    return __metadata_profiling


def set_remote_address(address):
    """ Enables the user to set the address of the GMQL remote service

    :param address: a string representing the URL of GMQL remote service
    :return: None
    """
    global __remote_address
    __remote_address = address


def get_remote_address():
    global __remote_address
    return __remote_address


def get_folders():
    global __folders
    return __folders


def init_settings():
    global __version__, __folders
    __version__ = get_version()
    __folders = TempFileManager.initialize_tmp_folders()
    initialize_user_folder()

