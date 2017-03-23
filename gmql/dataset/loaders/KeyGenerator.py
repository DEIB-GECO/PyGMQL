import hashlib


def generateKey(path):
    """
    Given a file name, generates the key of the sample
    :param path: file path
    :return: md5 hash of the file -> the index of the sample
    """
    # remove the .meta extension if present
    if path.endswith(".meta"):
        path = path[-5:]
    if path.startwith("file:"):
        path = path[5:]
    return hashlib.md5(path.encode())
