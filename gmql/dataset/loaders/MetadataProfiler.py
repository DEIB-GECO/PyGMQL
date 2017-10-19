from glob import glob
import os
from tqdm import tqdm
import strconv
from functools import reduce


class MetadataProfile:

    def __init__(self, metadata_info):
        if isinstance(metadata_info, dict):
            self.metadata_info = metadata_info
        else:
            raise TypeError("metadata_info must be a dictionary. "
                            "{} was provided".format(type(metadata_info)))


def create_metadata_profile(dataset_path):
    meta_files = glob(pathname=dataset_path + '/*.meta')
    from ... import disable_progress

    profile = dict()

    for mf in tqdm(meta_files, disable=disable_progress):
        full_mf_path = os.path.abspath(mf)
        fo = open(full_mf_path)
        lines = fo.readlines()
        fo_analysis = reduce(lambda x, y: x.update(y) or x,
                             map(analyze_line, lines), {})
        profile.update(fo_analysis)
    return MetadataProfile(metadata_info=profile)


def analyze_line(line):
    fields = line.split("\t")
    name = fields[0]
    value = fields[1]
    value_type = strconv.infer(value, converted=True)
    if value_type is None:
        value_type = str
    result = {name: value_type}
    return result