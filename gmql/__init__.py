"""
Setting up the environment of the library
"""
import logging
import subprocess as sp
from sys import stdout
from py4j.java_gateway import JavaGateway, GatewayParameters
from py4j.java_collections import ListConverter
import os
import time
import atexit
from pkg_resources import resource_filename
import psutil


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
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(handler)
    return logger

disable_progress = False


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
    global disable_progress
    if isinstance(how, bool):
        disable_progress = ~how
    else:
        raise ValueError("how must be a boolean. {} was found".format(type(how)))


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
        raise ValueError("how must be a boolean. {} was found".format(type(how)))


def start_gateway_server(gmql_jar, instances_file):
    port_n = check_instances(instances_file)

    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise SystemError("The environment variable JAVA_HOME is not setted")
    java_path = os.path.join(java_home, "bin", "java")
    gmql_jar_fn = resource_filename("gmql", os.path.join("resources", gmql_jar))
    command = [java_path, '-jar', gmql_jar_fn, str(port_n)]
    proc = sp.Popen(command)
    synchronize()
    gateway = JavaGateway(gateway_parameters=GatewayParameters(auto_convert=True, port=port_n))
    return proc, gateway, port_n


def check_instances(instances_file):
    instances_fn = resource_filename("gmql", os.path.join("resources", instances_file))
    port = 25335
    ports = get_open_instances()
    if len(ports) > 0:
        port = ports[-1] + 1
    with open(instances_fn, "a") as f:
        f.write(str(port) + os.linesep)
    return port


def remove_instance(port_n, instances_file):
    instances_fn = resource_filename("gmql", os.path.join("resources", instances_file))
    ports = get_open_instances()
    if port_n not in ports:
        raise ValueError("Port number {} is not in the current instances".format(port_n))
    ports.remove(port_n)
    ports = list(map(lambda x: str(x) + os.linesep, ports))
    with open(instances_fn, "w") as f:
        f.writelines(ports)


def get_open_instances():
    global instances_file
    instances_fn = resource_filename("gmql", os.path.join("resources", instances_file))
    ports = None
    with open(instances_fn, "r") as f:
        ports = list(map(int, f.readlines()))
    return ports


def synchronize():
    on = False
    while not on:
        try:
            f = open(synchfile, 'r')
            on = True
            f.close()
            os.remove(synchfile)
        except Exception:
            time.sleep(2)
            continue
    return


def get_python_api_package(gateway):
    return gateway.jvm.it.polimi.genomics.pythonapi


def start_gmql_manager(python_api_package):
    pythonManager = python_api_package.PythonManager
    pythonManager.startEngine()
    return pythonManager


def get_gateway():
    global gateway
    return gateway


def get_python_manager():
    global pythonManager
    return pythonManager
"""
    GMQL Logger configuration
"""
logger_name = "PyGML logger"
logger = set_logger(logger_name)

"""
    Initializing the JVM with the 
"""
synchfile = 'sync.txt'
gmql_jar = "pythonAPI.jar"
instances_file = "instances"
server_process, gateway, pythonManager, port_n = None, None, None, None


def start():
    global server_process, gateway, pythonManager, instances_file, port_n
    process_cleaning()
    server_process, gateway, port_n = start_gateway_server(gmql_jar, instances_file)
    python_api_package = get_python_api_package(gateway)
    pythonManager = start_gmql_manager(python_api_package)


def process_cleaning():
    global gmql_jar
    active_ports = get_open_instances()
    gmql_jar_name = resource_filename("gmql", os.path.join("resources", gmql_jar))
    for p in psutil.process_iter():
        name = p.name()
        if name == 'java':
            cmd = p.cmdline()
            if len(cmd) == 4 and cmd[2] == gmql_jar_name:
                port = int(cmd[3])
                if port not in active_ports:
                    logger.info("Previous JVM killed: {}".format(cmd[2]))
                    p.kill()   # kill it
                else:
                    active_ports.remove(port)
    global instances_file
    for a in active_ports:
        remove_instance(a, instances_file)



def stop():
    global pythonManager, server_process, gateway, port_n, instances_file
    remove_instance(port_n, instances_file)
    try:
        os.remove(synchfile)
    except Exception:
        pass
    try:
        gateway.shutdown()
    except Exception:
        pass
    try:
        process_cleaning()
    except Exception:
        pass

atexit.register(stop)

class GMQLManagerNotInitializedError(Exception):
    pass



# Starting the GMQL manager
start()


"""
    LANGUAGE CONVERSIONS
"""


def to_java_list(l):
    ListConverter().convert(l, gateway)

none = pythonManager.getNone()


def Some(thing):
    return pythonManager.getSome(thing)

"""
    EXPOSING INTERNAL FEATURES
"""
from .dataset.GMQLDataset import GMQLDataset, materialize   # the dataset
from .dataset.GDataframe import GDataframe, from_pandas     # the genomic dataframe
from .dataset.loaders.Loader import load_from_path
from .dataset import parsers                                # the set of parsers
from .dataset.DataStructures.Aggregates import *            # the possible aggregations
from .dataset.DataStructures.GenometricPredicates import *  # the possible join conditions
from .RemoteConnection.RemoteManager import RemoteManager      # for interacting with the remote cluster
