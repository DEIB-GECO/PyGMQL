import os

"""
    Generation of the index of the pandas dataframe.
    This can be done in different ways:
    - hashing the complete file name
    - using directly the file as index (this is visually appealing :) )
"""


def generateHashKey(filename):
    return hash(filename)


def generateNameKey(filename):
    filename = os.path.basename(filename)
    if filename.endswith(".meta"):
        return filename[:-5]
    else:
        return filename
