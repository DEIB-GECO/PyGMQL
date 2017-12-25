from glob import glob
import os
from tqdm import tqdm
import strconv
import pandas as pd


class MetadataProfile:

    def __init__(self, metadata_info):
        if isinstance(metadata_info, dict):
            self.metadata_info = metadata_info
        else:
            raise TypeError("metadata_info must be a dictionary. "
                            "{} was provided".format(type(metadata_info)))

    def get_metadata_type(self, metadata_attribute):
        if self.exists(metadata_attribute):
            t = self.metadata_info[metadata_attribute]
            return t
        else:
            raise ValueError("Metadata attribute {} not present".format(metadata_attribute))

    def get_metadata(self):
        return list(self.metadata_info.keys())

    def exists(self, metadata_attribute):
        return metadata_attribute in self.metadata_info.keys()

    def remove_attributes(self, l):
        if isinstance(l, str):
            if self.exists(l):
                self.metadata_info.pop(l)
            else:
                raise ValueError("Attribute {} doesn't exists".format(l))
        elif isinstance(l, list):
            for a in l:
                if self.exists(a):
                    self.metadata_info.pop(a)
                else:
                    raise ValueError("Attribute {} doesn't exists".format(a))
        else:
            raise TypeError("Input must be a string or a list")

    def select_attributes(self, l):
        if isinstance(l, list):
            res = dict()
            for a in l:
                if isinstance(a, str):
                    if self.exists(a):
                        res[a] = self.metadata_info[a]
                    else:
                        raise ValueError("Attribute {} doesn't exists".format(a))
                else:
                    raise TypeError("Attribute names must be strings")
        else:
            raise TypeError("Input must be a list")
        self.metadata_info = res

    def add_metadata(self, d):
        self.metadata_info.update(d)

    def to_df(self):
        df = pd.DataFrame.from_dict(self.metadata_info, orient="index")
        df.columns = ['Type', 'Values']
        # df['Type'] = df['Type'].map(str)
        df = df.sort_index()
        return df

    def show(self):
        print(self.to_df())

    def _repr_html_(self):
        return self.to_df().to_html()


def create_metadata_profile(dataset_path):
    meta_files = glob(pathname=dataset_path + '/*.meta')
    from ... import __disable_progress

    profile = dict()

    for mf in tqdm(meta_files, disable=__disable_progress):
        # print(mf + "\n\n")
        full_mf_path = os.path.abspath(mf)
        fo = open(full_mf_path)
        lines = fo.readlines()
        for l in lines:
            analyze_line(l, profile)
    return MetadataProfile(metadata_info=profile)


def analyze_line(line, d):
    fields = line.split("\t")
    name = fields[0]
    value = fields[1].strip()
    value_type = strconv.infer(value, converted=True)
    if value_type not in [str, int, float]:
        value_type = str
    # print("{} - {}".format(name, value))
    if name not in d.keys():
        d[name] = (value_type, set([value_type(value)]))
    else:
        current_type = d[name][0]
        # not agreement between types
        if current_type != value_type:
            # if one of the two is a string...str always wins
            if value_type == str or current_type == str:
                d[name] = (str, d[name][1])
            # between int and float...float always wins
            elif (current_type == int and value_type == float) or \
                 (current_type == float and value_type == int):
                d[name] = (float, d[name][1])
        d[name][1].add(value_type(value))
