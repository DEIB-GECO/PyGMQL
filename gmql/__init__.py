from .dataset.GDataframe import from_pandas
from .dataset.loaders.Loader import load_from_path, load_from_remote, load
from .dataset import parsers
from .dataset.DataStructures.Aggregates import *
from .dataset.DataStructures.ExpressionNodes import *
from .dataset.DataStructures.GenometricPredicates import *
from .RemoteConnection.RemoteManager import RemoteManager
# from . import ml

from .settings import init_settings as __init_settings
from .managers import init_managers as __init_managers
from .settings import set_remote_address, set_meta_profiling, set_mode, set_progress
from .managers import login, logout, get_remote_address, get_session_manager

__init_settings()
__init_managers()