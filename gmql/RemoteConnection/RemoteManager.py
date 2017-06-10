from . import default_address, headers
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoderMonitor, MultipartEncoder
import logging
import json
import pandas as pd
from ..dataset.parsers.BedParser import BedParser
from ..dataset.DataStructures import chr_aliases, start_aliases, stop_aliases, strand_aliases
from ..dataset.loaders import Loader
import os
from tqdm import tqdm
import shutil
import zipfile

# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1
#
# # You must initialize logging, otherwise you'll not see debug output.
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True


class RemoteManager:
    """ Manager of the user connection with the remote GMQL service
    """
    def __init__(self, address=None):
        """ Instantiate a new RemoteManager. If no address is provided, the default address will
        be used, which represents the current server hosted by the Dept. of Electronics, Information
        and Bioengineering of Politecnico di Milano.

        :param address: (optional) the address of the remote GMQL service
        """
        if address is None:
            self.address = default_address
        self.logger = logging.getLogger("PyGML logger")
        self.auth_token = None
        self.json_encoder = json.JSONEncoder()

    def login(self, username=None, password=None):
        """ Before doing any remote operation, the user has to login to the GMQL serivice.
        This can be done in the two following ways:

            * Guest mode: the user has no credentials and uses the system only as a temporary guest
            * Authenticated mode: the users has credentials and a stable remote account

        If neither username and password are specified, the user enters the system as a guest.
        If both are specified and they correspond to an existent user, the user enters as an
        authenticated user

        :param username: (optional)
        :param password: (optional)
        :return: None
        """
        if (username is None) and (password is None):
            auth_token = self._login_guest()
        elif (username is not None) and (password is not None):
            auth_token, fullName = self._login_credentials(username, password)
            self.logger.info("You are logged as {}".format(fullName))
        else:
            raise ValueError("you have to specify both username and password or nothing")

        if auth_token is not None:
            self.logger.info("Auth-token: {}".format(auth_token))
            self.auth_token = auth_token
        else:
            raise ConnectionError("Impossible to retrieve the authentication token")

    def _login_guest(self):
        url = self.address + "/guest"
        response = requests.get(url=url, headers=headers)
        response = response.json()
        return response.get("authToken")

    def _login_credentials(self, username, password):
        url = self.address + "/login"
        body = {
            "username": username,
            "password": password
        }
        # body = self.json_encoder.encode(body)
        response = requests.post(url, data=json.dumps(body), headers=headers)
        response = response.json()

        errorString = response.get("errorString")
        if errorString is not None:
            raise ValueError(errorString)
        else:
            auth_token = response.get("authToken")
            fullName = response.get("fullName")
        return auth_token, fullName

    def get_dataset_list(self):
        """ Returns the list of available datasets for the current user.

        :return: a pandas Dataframe
        """
        url = self.address + "/datasets"
        header = self._check_authentication()
        response = requests.get(url, headers=header)
        response = response.json()
        datasets = response.get("datasets")
        return pd.DataFrame.from_dict(datasets)

    def get_dataset_samples(self, dataset_name):
        """ Get the list of samples of a specific remote dataset.

        :param dataset_name: the dataset name
        :return: a pandas Dataframe
        """
        url = self.address + "/datasets/"+dataset_name
        header = self._check_authentication()
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        response = response.json()
        samples = response.get("samples")
        return pd.DataFrame.from_dict(samples)

    def get_dataset_schema(self, dataset_name):
        """ Given a dataset name, it returns a BedParser coherent with the schema of it

        :param dataset_name: a dataset name on the repository
        :return: a BedParser
        """
        url = self.address + "/datasets/" + dataset_name+"/schema"
        header = self._check_authentication()
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        response = response.json()
        name = response.get("name")
        schemaType = response.get("schemaType")
        if schemaType.lower() != 'bed':
            raise TypeError("This dataset is not of type BED. {} was found".format(schemaType))
        fields = response.get("fields")
        chrPos, startPos, stopPos, strandPos = None, None, None, None
        otherPos = []
        for i,f in enumerate(fields):
            fieldName = f.get("name")
            fieldType = f.get("fieldType")
            if fieldName in chr_aliases:
                chrPos = i
            elif fieldName in start_aliases:
                startPos = i
            elif fieldName in stop_aliases:
                stopPos = i
            elif fieldName in strand_aliases:
                strandPos = i
            else:
                otherPos.append((i, fieldName, fieldType.lower()))

        return BedParser(parser_name=name, chrPos=chrPos, startPos=startPos,
                         stopPos=stopPos, strandPos=strandPos, otherPos=otherPos)

    def upload_dataset(self, dataset_local_path, dataset_name):
        """ Upload to the repository an entire dataset from a local path

        :param dataset_local_path: the local path of the dataset
        :param dataset_name: the name you want to assign to the dataset remotely
        :return: None
        """
        url = self.address + "/datasets/" + dataset_name + "/uploadSample"
        header = self._check_authentication()
        file_paths, schema_path = Loader.get_file_paths(dataset_local_path)
        fields = dict()
        fields['schema'] = (os.path.basename(schema_path), open(schema_path, "rb"), 'application/octet-stream')
        for i, file in enumerate(file_paths):
            fields["file"+str(i + 1)] = (os.path.basename(file), open(file, "rb"), 'application/octet-stream')

        encoder = MultipartEncoder(fields)
        callback = create_callback(encoder, len(fields))

        m_encoder = MultipartEncoderMonitor(encoder, callback)

        header['Content-Type'] = m_encoder.content_type
        params = {"schemaName": "bed"}

        self.logger.info("Uploading dataset at {} with name {}".format(dataset_local_path, dataset_name))

        response = requests.post(url, data=m_encoder,
                                 headers=header,
                                 params=params)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.content))

    def delete_dataset(self, dataset_name):
        """ Deletes the dataset having the specified name

        :param dataset_name: the name that the dataset has on the repository
        :return: None
        """
        url = self.address + "/datasets/" + dataset_name
        header = self._check_authentication()
        response = requests.delete(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        self.logger.info("Dataset {} was deleted from the repository".format(dataset_name))

    def _check_authentication(self):
        if self.auth_token is not None:
            header = headers.copy()
            header['X-AUTH-TOKEN'] = self.auth_token
            return header
        else:
            raise EnvironmentError("you first need to login before doing operations")

    def download_dataset(self, dataset_name, local_path):
        """ It downloads from the repository the specified dataset and puts it
        in the specified local folder

        :param dataset_name: the name the dataset has in the repository
        :param local_path: where you want to save the dataset
        :return: None
        """
        url = self.address + "/datasets/" + dataset_name + "/zip"
        header = self._check_authentication()
        self.logger.info("Downloading dataset {} to {}".format(dataset_name, local_path))
        response = requests.get(url, stream=True, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        if os.path.isdir(local_path):
            shutil.rmtree(local_path)
        os.mkdir(local_path)
        tmp_zip = os.path.join(local_path, "tmp.zip")
        f = open(tmp_zip, "wb")
        for chunk in tqdm(response.iter_content(chunk_size=512)):
            if chunk:
                f.write(chunk)
        f.close()
        with zipfile.ZipFile(tmp_zip, "r") as zip_ref:
            zip_ref.extractall(local_path)
        os.remove(tmp_zip)


def create_callback(encoder, n_files=None):
    encoder_len = encoder.len
    if n_files is not None:
        tot_len = n_files
        byte_per_file = encoder_len / n_files
    else:
        tot_len = encoder_len
    bar = tqdm(total=tot_len)

    if n_files is not None:
        def callback(monitor):
            bar.update(max(int((monitor.bytes_read / byte_per_file) - bar.n), 0))

        return callback

    def callback(monitor):
        bar.update(monitor.bytes_read - bar.n)

    return callback
