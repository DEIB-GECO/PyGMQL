"""
Setting up the environment of the library
"""
import logging
import subprocess as sp
from sys import stdout
from py4j.java_gateway import JavaGateway
import os


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
    java_path = os.path.join(java_home, "bin", "java")

    command = [java_path, '-jar', gmql_jar]
    proc = sp.Popen(command)

    gateway = JavaGateway()
    return proc, gateway


def get_python_api_package(gateway):
    return gateway.jvm.it.polimi.genomics.pythonapi


def start_gmql_manager(python_api_package):
    gmql_manager = python_api_package.GMQLManager
    gmql_manager.startEngine()
    return gmql_manager

"""
    GMQL Logger configuration
"""

logger_name = "PyGML logger"
logger = set_logger(logger_name)

"""
    Initializing the JVM with the 
"""
gmql_jar = "/home/luca/Documenti/GMQL/GMQL-PythonAPI/target/uber-GMQL-PythonAPI-1.0-SNAPSHOT.jar"
server_process, gateway = start_gateway_server(gmql_jar)

"""
    Starting the GMQL manager
"""
python_api_package = get_python_api_package(gateway)
gmql_manager = start_gmql_manager(python_api_package)