from .FileManagment import TempFileManager, initialize_user_folder, get_resources_dir
from .configuration import Configuration
import os
import sys


__version__ = None
__progress_bar = True
__metadata_profiling = False
__remote_address = None
__mode = "local"
__master = "local"
__gcloud_token = None
__folders = None
__init_configs_local = {
    "spark.serializer": 'org.apache.spark.serializer.KryoSerializer',
    'spark.executor.memory': '6g',
    'spark.driver.memory': '8g',
    'spark.kryoserializer.buffer.max': '1g',
    'spark.driver.maxResultSize': '5g'
}
__regions_batch_size = 10000

__init_configs = __init_configs_local.copy()
__init_configs_local.update({
    'spark.driver.host': 'localhost',
    'spark.local.dir': '/tmp'
})
__configuration = None


def get_configuration():
    global __configuration
    return __configuration


def get_init_config():
    global __init_configs
    return __init_configs


def set_regions_batch_size(batch: int):
    global __regions_batch_size
    __regions_batch_size = batch


def get_regions_batch_size():
    global __regions_batch_size
    return __regions_batch_size


def set_configuration(conf):
    global __configuration
    if not isinstance(conf, Configuration):
        raise TypeError("Configuration expected. {} was found".format(type(conf)))
    __configuration = conf


def set_master(master: str):
    global __master
    __master = master


def get_master():
    global __master
    return __master


def set_gcloud_token(token: str):
    global __gcloud_token
    __gcloud_token = token


def get_gcloud_token():
    global __gcloud_token
    return __gcloud_token


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
    version_file_name = os.path.join(get_resources_dir(),  "version")
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


def initialize_configuration():
    global __configuration, __init_configs_local
    configs = Configuration()
    configs.set_spark_conf(d=__init_configs_local)
    __configuration = configs


def init_settings():
    global __version__, __folders, __configuration
    __version__ = get_version()
    initialize_user_folder()
    __folders = TempFileManager.initialize_tmp_folders()
    initialize_configuration()
    __configuration.set_spark_conf("spark.local.dir", __folders['spark'])
    if sys.platform.startswith("win32"):
        # if we are on windows set the hadoop home to winutils.exe
        hadoop_folder_fn = os.path.join(get_resources_dir(), "hadoop")
        __configuration.set_system_conf("hadoop.home.dir", hadoop_folder_fn)
