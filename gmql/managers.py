from .FileManagment.DependencyManager import DependencyManager
from .RemoteConnection.SessionManager import load_sessions, store_sessions
from .FileManagment import TempFileManager
from .settings import get_folders, get_remote_address
import py4j
from py4j.java_gateway import JavaGateway, launch_gateway, GatewayParameters
from pkg_resources import resource_filename
import os
import sys
import time
import atexit


__remote_manager = None
__session_manager = None
__dependency_manager = None
__source_table = None
__gateway = None
__pythonManager = None


def start():
    global __pythonManager, __gateway, __dependency_manager

    java_home = os.environ.get("JAVA_HOME")
    if java_home is None:
        raise SystemError("The environment variable JAVA_HOME is not set")
    java_path = os.path.join(java_home, "bin", "java")
    gmql_jar_fn = __dependency_manager.resolve_dependencies()
    py4j_jar = __check_py4j_backend()
    _port = launch_gateway(classpath=gmql_jar_fn, die_on_exit=True,
                           java_path=java_path, javaopts=['-Xmx4096m'],
                           jarpath=py4j_jar)
    __gateway = JavaGateway(gateway_parameters=GatewayParameters(port=_port,
                                                                 auto_convert=True))
    python_api_package = get_python_api_package(__gateway)
    __pythonManager = start_gmql_manager(python_api_package)

    if sys.platform.startswith("win32"):
        # if we are on windows set the hadoop home to winutils.exe
        hadoopFolder_fn = resource_filename(
            "gmql", os.path.join("resources", "hadoop")
        )
        __pythonManager.setHadoopHomeDir(hadoopFolder_fn)

    # setting the spark tmp folder
    folders = get_folders()
    __pythonManager.setSparkLocalDir(folders['spark'])


def __check_py4j_backend():
    py4j_version = py4j.__version__
    py4j_backend_jar = resource_filename("gmql", os.path.join("resources", "py4j-{}.jar".format(py4j_version)))
    if not os.path.isfile(py4j_backend_jar):
        py4j_location = DependencyManager.find_package(
                            repo="https://oss.sonatype.org/content/repositories/releases/",
                            repo_name="releases",
                            groupId="net.sf.py4j",
                            artifactId="py4j",
                            version=py4j_version,
                        )
        DependencyManager.download_from_location(py4j_location, py4j_backend_jar)
    return py4j_backend_jar


def stop():
    global __gateway, __session_manager
    # flushing the tmp files
    TempFileManager.flush_everything()
    store_sessions(__session_manager.sessions)
    if __gateway is not None:
        __gateway.shutdown()


def get_python_api_package(gateway):
    return gateway.jvm.it.polimi.genomics.pythonapi


def start_gmql_manager(python_api_package):
    pythonManager = python_api_package.PythonManager
    pythonManager.startEngine()
    return pythonManager


def get_gateway():
    global __gateway

    if __gateway is None:
        # Starting the GMQL manager
        start()
        return __gateway
    else:
        return __gateway


def get_python_manager():
    global __pythonManager

    if __pythonManager is None:
        # Starting the GMQL manager
        start()
        return __pythonManager
    else:
        return __pythonManager


def __initialize_source_table():
    global __source_table
    from .dataset.loaders.Sources import SourcesTable
    __source_table = SourcesTable()


def get_source_table():
    global __source_table
    return __source_table


def __initialize_dependency_manager():
    global __dependency_manager
    __dependency_manager = DependencyManager()


def __initialize_session_manager():
    global __session_manager
    __session_manager = load_sessions()


def login():
    """ Enables the user to login to the remote GMQL service.
    If both username and password are None, the user will be connected as guest.
    """
    from .RemoteConnection.RemoteManager import RemoteManager
    global __remote_manager, __session_manager
    remote_address = get_remote_address()
    res = __session_manager.get_session(remote_address)
    if res is None:
        # there is no session for this address, let's login as guest
        rm = RemoteManager(address=remote_address)
        rm.login()
        session_type = "guest"
    else:
        # there is a previous session for this address, let's do an auto login
        # using that access token
        rm = RemoteManager(address=remote_address, auth_token=res[1])
        # if the access token is not valid anymore (therefore we are in guest mode)
        # the auto_login function will perform a guest login from scratch
        session_type = rm.auto_login(how=res[2])
    # store the new session
    __remote_manager = rm
    access_time = int(time.time())
    auth_token = rm.auth_token
    __session_manager.add_session(remote_address, auth_token, access_time, session_type)


def logout():
    """ The user can use this command to logout from the remote service

    :return: None
    """
    global __remote_manager
    __remote_manager.logout()


def execute_remote():
    global __remote_manager
    __remote_manager.execute_remote_all()


def get_remote_manager():
    """ Returns the current remote manager

    :return: a RemoteManager
    """
    global __remote_manager
    return __remote_manager


def get_session_manager():
    global __session_manager
    return __session_manager


def init_managers():
    atexit.register(stop)
    __initialize_source_table()
    __initialize_session_manager()
    __initialize_dependency_manager()
