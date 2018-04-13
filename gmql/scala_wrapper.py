from py4j.java_collections import ListConverter
from .managers import get_gateway, get_python_manager


def to_java_list(l):
    gateway = get_gateway()
    ListConverter().convert(l, gateway)


def none():
    return get_python_manager().getNone()


def Some(thing):
    return get_python_manager().getSome(thing)
