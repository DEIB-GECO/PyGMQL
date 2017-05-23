"""
Setting up the environment of the library
"""
import logging
import subprocess as sp
from sys import stdout
from py4j.java_gateway import JavaGateway
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


def start_gateway_server(gmql_jar):
    """
    Starts the JVM with the server handling the command requests from the user
    :param gmql_jar: the location of the jar containing the Scala API
    :return: the process of the server and the gateway for python for accessing to it
    """
    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise SystemError("The environment variable JAVA_HOME is not setted")
    java_path = os.path.join(java_home, "bin", "java")
    gmql_jar_fn = resource_filename("gmql", "resources/"+gmql_jar)
    command = [java_path, '-jar', gmql_jar_fn]
    proc = sp.Popen(command)
    synchronize()
    gateway = JavaGateway()
    return proc, gateway


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
server_process, gateway, pythonManager = None, None, None


def start():
    global server_process, gateway, pythonManager
    process_cleaning()
    server_process, gateway = start_gateway_server(gmql_jar)
    python_api_package = get_python_api_package(gateway)
    pythonManager = start_gmql_manager(python_api_package)


def process_cleaning():
    global gmql_jar
    gmql_jar_name = resource_filename("gmql", "resources/" + gmql_jar)
    for p in psutil.process_iter():
        name = p.name()
        if name == 'java':
            cmd = p.cmdline()
            if len(cmd) == 3 and cmd[2] == gmql_jar_name:
                p.terminate() # kill it


def stop():
    global pythonManager, server_process

    try:
        os.remove(synchfile)
    #     if pythonManager is None:
    #         raise GMQLManagerNotInitializedError("You need first to initialize the GMQLManager with the start() method")
    except Exception:
        pass
    try:
        pythonManager.stopEngine()
    except Exception:
        pass
    try:
        server_process.terminate()
    except Exception:
        pass

atexit.register(stop)

# things to expose to the user
from .dataset.GMQLDataset import GMQLDataset                # the dataset
from .dataset.loaders.Loader import load_from_path
from .dataset import parsers                                # the set of parsers
from .dataset.DataStructures.Aggregates import *            # the possible aggregations
from .dataset.DataStructures.GenometricPredicates import *  # the possible join conditions


class GMQLManagerNotInitializedError(Exception):
    pass


"""
    Starting the GMQL manager
"""
start()
