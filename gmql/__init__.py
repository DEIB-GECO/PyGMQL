"""
Setting up the environment of the library
"""
import logging, os, atexit
from sys import stdout
from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters
from py4j.java_collections import ListConverter
from .FileManagment import TempFileManager
from pkg_resources import resource_filename
from tqdm import tqdm
import requests

"""
    Version management
"""


def get_version():
    version_file_name = resource_filename("gmql", os.path.join("resources", "version"))
    with open(version_file_name, "r") as f_ver:
        version = f_ver.read().strip()
    return version

__version__ = get_version()


def __get_github_url():
    github_fn = resource_filename("gmql", os.path.join("resources", "github_url"))
    with open(github_fn, "r") as f_ver:
        url = f_ver.read().strip()
    return url


__gmql_jar = "pythonAPI-{}.jar".format(__version__)
__backend_download_url = __get_github_url()

"""
    Logging and progress bars
"""


def set_logger(logger_name):
    """
    The library has a personal logger that tells the user the intermediate steps of the
    commands that he/she calls
    :param: name of the logger
    :return: the setup logger
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # create a stream handler
    handler = logging.StreamHandler(stdout)
    handler.setLevel(logging.INFO)

    # create a logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    return logger


__disable_progress = False


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
    global __disable_progress
    if isinstance(how, bool):
        __disable_progress = ~how
    else:
        raise ValueError(
            "how must be a boolean. {} was found".format(type(how)))


def is_progress_enabled():
    return not __disable_progress


def set_logging(how):
    """ Enables or disables the logging mechanism of PyGMQL.

    :param how: True if you want the logging, False otherwise
    :return: None

    Example::

        import gmql as gl

        gl.set_logging(True)   # abilitates the logging
        # ....do something...
        gl.set_logging(False)  # removes the logging
        # ....do something...
    """
    if isinstance(how, bool):
        if how:
            logging.disable(logging.NOTSET)
        else:
            logging.disable(logging.CRITICAL)
    else:
        raise ValueError(
            "how must be a boolean. {} was found".format(type(how)))


_metadata_profiling = True


def set_meta_profiling(how):
    """ Enables or disables the profiling of metadata at the loading of a GMQLDataset

    :param how: True if you want to analyze the metadata when a GMQLDataset is created
                by a load_from_*. False otherwise. (Default=True)
    :return: None
    """
    global _metadata_profiling
    if isinstance(how, bool):
        _metadata_profiling = how
    else:
        raise TypeError("how must be boolean. {} was provided".format(type(how)))


"""
    Backend management
"""


def __check_backend(gmql_jar_fn):
    if os.path.isfile(gmql_jar_fn):
        return gmql_jar_fn
    else:
        # we need to download it
        global __backend_download_url, __version__, __gmql_jar
        full_url = '{}/{}/{}/{}'.format(__backend_download_url,
                                        "releases/download",
                                        __version__, __gmql_jar)
        logger.info("Downloading updated backend version ({})".format(full_url))

        r = requests.get(full_url, stream=True)
        total_size = int(r.headers.get('content-length', 0))
        chunk = 1024*1024
        with open(gmql_jar_fn, "wb") as f:
            for data in tqdm(r.iter_content(chunk), total=total_size/chunk, unit='B', unit_scale=True):
                f.write(data)
        return gmql_jar_fn


def __get_python_api_package(gateway):
    return gateway.jvm.it.polimi.genomics.pythonapi


def __start_gmql_manager(python_api_package):
    pythonManager = python_api_package.PythonManager
    pythonManager.startEngine()
    return pythonManager


def _get_gateway():
    global gateway

    if gateway is None:
        # Starting the GMQL manager
        start()
        return gateway
    else:
        return gateway


def get_python_manager():
    global pythonManager

    if pythonManager is None:
        # Starting the GMQL manager
        start()
        return pythonManager
    else:
        return pythonManager

"""
    GMQL Logger configuration
"""
logger_name = "PyGMQL logger"
logger = set_logger(logger_name)

"""
    Initializing the JVM with the 
"""
gateway, pythonManager = None, None


def start():
    global pythonManager, gateway

    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise SystemError("The environment variable JAVA_HOME is not set")
    java_path = os.path.join(java_home, "bin", "java")

    gmql_jar_fn = resource_filename(
        "gmql", os.path.join("resources", __gmql_jar))
    gmql_jar_fn = __check_backend(gmql_jar_fn)

    _port = launch_gateway(classpath=gmql_jar_fn, die_on_exit=True,
                           java_path=java_path, javaopts=['-Xmx4096m'])
    # gateway = JavaGateway.launch_gateway(classpath=gmql_jar_fn, die_on_exit=True,
    #                                      java_path=java_path)
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=_port,
                                                               auto_convert=True))
    python_api_package = __get_python_api_package(gateway)
    pythonManager = __start_gmql_manager(python_api_package)

    # setting the spark tmp folder
    pythonManager.setSparkLocalDir(folders['spark'])


def stop():
    global gateway
    # flushing the tmp files
    TempFileManager.flush_everything()
    if gateway is not None:
        gateway.shutdown()


atexit.register(stop)


class GMQLManagerNotInitializedError(Exception):
    pass


# Setting up the temporary files folder
folders = TempFileManager.initialize_tmp_folders()


"""
    LANGUAGE CONVERSIONS
"""


def to_java_list(l):
    ListConverter().convert(l, gateway)


def none():
    return pythonManager.getNone()


def Some(thing):
    return pythonManager.getSome(thing)

"""
    Remote manager management
"""

remote_manager = None


def get_remote_manager():
    """ Returns the current remote manager

    :return: a RemoteManager
    """
    global remote_manager
    return remote_manager


def __initialize_remote_manager(address=None):
    global remote_manager
    remote_manager = RemoteManager(address)


def login(username=None, password=None):
    """ Enables the user to login to the remote GMQL service.
    If both username and password are None, the user will be connected as guest.

    :param username: (optional) the username
    :param password: (optional) the password
    :return: None
    """
    global remote_manager
    remote_manager.login(username, password)


def set_remote_address(address):
    """ Enables the user to set the address of the GMQL remote service

    :param address: a string representing the URL of GMQL remote service
    :return: None
    """
    global remote_manager
    remote_manager.address = address


def logout():
    """ The user can use this command to logout from the remote service

    :return: None
    """
    global remote_manager
    remote_manager.logout()


def execute_remote():
    global remote_manager
    remote_manager.execute_remote_all()


"""
    Execution mode management
"""

mode = "local"


def set_mode(how):
    """ Sets the behavior of the API

    :param how: if 'remote' all the execution is performed on the remote server; if 'local' all
           it is executed locally. Default = 'local'
    :return: None
    """
    global mode
    if how == "local":
        mode = how
    elif how == "remote":
        mode = how
    else:
        raise ValueError("how must be 'local' or 'remote'")


def get_mode():
    global mode
    return mode


"""
    Datasets management
"""

__source_table = None


def __initialize_source_table():
    global __source_table
    from .dataset.loaders.Sources import SourcesTable
    __source_table = SourcesTable()


def _get_source_table():
    global __source_table
    return __source_table


"""
    EXPOSING INTERNAL FEATURES
"""
from .dataset.GDataframe import from_pandas
from .dataset.loaders.Loader import load_from_path, load_from_remote, load
from .dataset import parsers
# the possible aggregations
from .dataset.DataStructures.Aggregates import *
from .dataset.DataStructures.ExpressionNodes import *
# the possible join conditions
from .dataset.DataStructures.GenometricPredicates import *
# for interacting with the remote cluster
from .RemoteConnection.RemoteManager import RemoteManager

# modules for the machine learning library
from . import ml


__initialize_remote_manager()
__initialize_source_table()

