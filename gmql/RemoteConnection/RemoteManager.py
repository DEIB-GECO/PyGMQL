from . import default_address, headers
from .. import get_python_manager, is_progress_enabled
import requests, time, logging, json
from requests_toolbelt.multipart.encoder import MultipartEncoderMonitor, MultipartEncoder
import pandas as pd
from ..dataset.GDataframe import GDataframe
from ..dataset.parsers.BedParser import BedParser
from ..dataset.parsers import allowed_types
from ..dataset.DataStructures import chr_aliases, start_aliases, stop_aliases, strand_aliases
from ..dataset.loaders import Loader
from ..FileManagment import TempFileManager
import os, shutil, zipfile, sys
from tqdm import tqdm
from ..dataset.storers.parserToXML import parserToXML


# import http.client as http_client
# http_client.HTTPConnection.debuglevel = 1
#
# # You must initialize logging, otherwise you'll not see debug output.
# logging.basicConfig()
# logging.getLogger().setLevel(logging.DEBUG)
# requests_log = logging.getLogger("requests.packages.urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

good_status = ['PENDING', 'RUNNING', 'DS_CREATION_RUNNING']


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
        else:
            self.address = address
        self.logger = logging.getLogger("PyGML logger")
        self.auth_token = None
        self.json_encoder = json.JSONEncoder()

    """
        Security controls
    """

    def register(self, first_name, last_name, user_name, email, password):
        url = self.address + "/register"
        body = {
            "firstName" : first_name,
            "lastName": last_name,
            "username": user_name,
            "email": email,
            "password": password
        }
        response = requests.post(url, data=json.dumps(body))
        if response.status_code != 200:
            raise ValueError("Code {}. {}".format(response.status_code, response.json().get("error")))

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
            auth_token = self.__login_guest()
        elif (username is not None) and (password is not None):
            auth_token, fullName = self.__login_credentials(username, password)
            self.logger.info("You are logged as {}".format(fullName))
        else:
            raise ValueError("you have to specify both username and password or nothing")

        if auth_token is not None:
            self.logger.info("Auth-token: {}".format(auth_token))
            self.auth_token = auth_token
        else:
            raise ConnectionError("Impossible to retrieve the authentication token")

    def __login_guest(self):
        url = self.address + "/guest"
        response = requests.get(url=url, headers=headers)
        response = response.json()
        return response.get("authToken")

    def __login_credentials(self, username, password):
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

    def __check_authentication(self):
        if self.auth_token is not None:
            header = headers.copy()
            header['X-AUTH-TOKEN'] = self.auth_token
            return header
        else:
            raise EnvironmentError("you first need to login before doing operations")

    def logout(self):
        """ Logout from the remote account

        :return: None
        """
        url = self.address + "/logout"
        header = self.__check_authentication()
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}. {}".format(response.status_code, response.json().get("error")))

    """
        Repository
    """

    def get_dataset_list(self):
        """ Returns the list of available datasets for the current user.

        :return: a pandas Dataframe
        """
        url = self.address + "/datasets"
        header = self.__check_authentication()
        response = requests.get(url, headers=header)
        response = response.json()
        datasets = response.get("datasets")
        return pd.DataFrame.from_dict(datasets)

    def get_dataset_samples(self, dataset_name, owner=None):
        """ Get the list of samples of a specific remote dataset.

        :param dataset_name: the dataset name
        :param owner: (optional) who owns the dataset. If it is not specified, the current user
               is used. For public dataset use 'public'.
        :return: a pandas Dataframe
        """
        if isinstance(owner, str):
            owner = owner.lower()
            dataset_name = owner + "." + dataset_name

        header = self.__check_authentication()

        url = self.address + "/datasets/" + dataset_name
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        response = response.json()
        samples = response.get("samples")
        return pd.DataFrame.from_dict(samples)

    def get_dataset_schema(self, dataset_name, owner=None):
        """ Given a dataset name, it returns a BedParser coherent with the schema of it

        :param dataset_name: a dataset name on the repository
        :param owner: (optional) who owns the dataset. If it is not specified, the current user
               is used. For public dataset use 'public'.
        :return: a BedParser
        """

        if isinstance(owner, str):
            owner = owner.lower()
            dataset_name = owner + "." + dataset_name

        url = self.address + "/datasets/" + dataset_name+"/schema"
        header = self.__check_authentication()
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))

        response = response.json()
        name = response.get("name")
        schemaType = response.get("type")
        if schemaType.lower() not in allowed_types:
            raise TypeError("This dataset is not of type {}. {} was found".format(allowed_types, schemaType))

        fields = response.get("fields")
        chrPos, startPos, stopPos, strandPos = None, None, None, None
        otherPos = []
        for i,f in enumerate(fields):
            fieldName = f.get("name").lower()
            fieldType = f.get("type").lower()
            if fieldName in chr_aliases:
                chrPos = i
            elif fieldName in start_aliases:
                startPos = i
            elif fieldName in stop_aliases:
                stopPos = i
            elif fieldName in strand_aliases:
                strandPos = i
            else:
                otherPos.append((i, fieldName, fieldType))

        return BedParser(parser_name=name, chrPos=chrPos, startPos=startPos, delimiter="\t",
                         stopPos=stopPos, strandPos=strandPos, otherPos=otherPos)

    def upload_dataset(self, dataset, dataset_name):
        """ Upload to the repository an entire dataset from a local path

        :param dataset: the local path of the dataset
        :param dataset_name: the name you want to assign to the dataset remotely
        :return: None
        """

        url = self.address + "/datasets/" + dataset_name + "/uploadSample"
        header = self.__check_authentication()

        fields = dict()
        remove = False
        if isinstance(dataset, GDataframe):
            tmp_path = TempFileManager.get_new_dataset_tmp_folder()
            dataset.to_dataset_files(local_path=tmp_path)
            dataset = tmp_path
            remove = True

        # a path is provided
        if not isinstance(dataset, str):
            raise TypeError("Dataset can be a path or a GDataframe. {} was passed".format(type(dataset)))

        file_paths, schema_path = Loader.get_file_paths(dataset)
        fields['schema'] = (os.path.basename(schema_path), open(schema_path, "rb"), 'application/octet-stream')
        for i, file in enumerate(file_paths):
            fields["file"+str(i + 1)] = (os.path.basename(file), open(file, "rb"), 'application/octet-stream')

        encoder = MultipartEncoder(fields)
        callback = create_callback(encoder, len(fields))

        m_encoder = MultipartEncoderMonitor(encoder, callback)

        header['Content-Type'] = m_encoder.content_type
        params = {"schemaName": "bed"}

        self.logger.info("Uploading dataset at {} with name {}".format(dataset, dataset_name))

        response = requests.post(url, data=m_encoder,
                                 headers=header,
                                 params=params)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.content))

        if remove:
            TempFileManager.delete_tmp_dataset(dataset)

    def delete_dataset(self, dataset_name):
        """ Deletes the dataset having the specified name

        :param dataset_name: the name that the dataset has on the repository
        :return: None
        """
        url = self.address + "/datasets/" + dataset_name
        header = self.__check_authentication()
        response = requests.delete(url, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        self.logger.info("Dataset {} was deleted from the repository".format(dataset_name))

    """
        Download repository
    """

    def download_dataset(self, dataset_name, local_path, how="stream"):
        """ It downloads from the repository the specified dataset and puts it
        in the specified local folder

        :param dataset_name: the name the dataset has in the repository
        :param local_path: where you want to save the dataset
        :param how: 'zip' downloads the whole dataset as a zip file and decompress it; 'stream'
               downloads the dataset sample by sample
        :return: None
        """
        if not os.path.isdir(local_path):
            os.makedirs(local_path)

        if how == 'zip':
            return self.download_as_zip(dataset_name, local_path)
        elif how == 'stream':
            return self.download_as_stream(dataset_name, local_path)
        else:
            raise ValueError("how must be {'zip', 'stream'}")

    def download_as_zip(self, dataset_name, local_path):
        header = self.__check_authentication()
        url = self.address + "/datasets/" + dataset_name + "/zip"
        self.logger.info("Downloading dataset {} to {}".format(dataset_name, local_path))
        response = requests.get(url, stream=True, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}: {}".format(response.status_code, response.json().get("error")))
        tmp_zip = os.path.join(local_path, "tmp.zip")
        f = open(tmp_zip, "wb")
        # TODO: find a better way to display the download progression
        for chunk in tqdm(response.iter_content(chunk_size=512), disable=not is_progress_enabled()):
            if chunk:
                f.write(chunk)
        f.close()
        with zipfile.ZipFile(tmp_zip, "r") as zip_ref:
            zip_ref.extractall(local_path)
        os.remove(tmp_zip)

    def download_as_stream(self, dataset_name, local_path):
        samples = self.get_dataset_samples(dataset_name)
        ids = samples.id.unique()
        # download the data
        for id in tqdm(ids, disable=not is_progress_enabled()):
            name = samples[samples.id == id].name.values[0]
            self.download_sample(dataset_name=dataset_name,
                                 sample_name=name,
                                 local_path=local_path,
                                 how="all",
                                 header=False)

        # download the schema
        schema = self.get_dataset_schema(dataset_name=dataset_name)
        parserToXML(parser=schema, datasetName=dataset_name, path=os.path.join(local_path, dataset_name + ".schema"))

    def download_sample(self, dataset_name, sample_name, local_path, how="all", header=False):
        header_get = self.__check_authentication()
        url = self.address + "/datasets/{}/{}/{}?header={}"
        region = False
        meta = False

        sample_path = os.path.join(local_path, sample_name)
        region_path = sample_path
        meta_path = sample_path + ".meta"

        if how == 'regs':
            region = True
        elif how == 'meta':
            meta = True
        elif how == 'all':
            region = True
            meta = True
        else:
            raise ValueError("how must be {'regs', 'meta', 'all'}")

        header = "true" if header else "false"

        if region:
            url_region = url.format(dataset_name, sample_name, "region", header)
            response = requests.get(url_region, stream=True, headers=header_get)
            with open(region_path, "wb") as f:
                f.write(response.content)

        if meta:
            url_meta = url.format(dataset_name, sample_name, "metadata", header)
            response = requests.get(url_meta, stream=True, headers=header_get)
            with open(meta_path, "wb") as f:
                f.write(response.content)

    """
        Query
    """

    def query(self, query, output_path=None, file_name="query", output="tab"):
        """ Execute a GMQL textual query on the remote server.

        :param query: the string containing the query
        :param output_path (optional): where to store the results locally. If specified
               the results are downloaded locally
        :param file_name (optional): the name of the query
        :param output (optional): how to save the results. It can be "tab" or "gtf"
        :return: a pandas dataframe with the dictionary ids of the results
        """
        header = self.__check_authentication()
        header['Content-Type'] = "text/plain"
        output = output.lower()
        if output not in ['tab', 'gtf']:
            raise ValueError("output must be 'tab' or 'gtf'")
        url = self.address + "/queries/run/" + file_name + '/' + output
        response = requests.post(url, data=query, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}. {}".format(response.status_code, response.json().get("error")))
        response = response.json()
        jobid = response.get("id")
        self.logger.info("JobId: {}. Waiting for the result".format(jobid))

        status_resp = self._wait_for_result(jobid)

        datasets = status_resp.get("datasets")
        result = []
        for dataset in datasets:
            name = dataset.get("name")
            result.append({'dataset': name})
            if output_path is not None:
                path = os.path.join(output_path, name)
                self.download_dataset(dataset_name=name, local_path=path)
        return pd.DataFrame.from_dict(result)

    def _wait_for_result(self, jobid):
        count = 1
        while True:
            status_resp = self.trace_job(jobid)
            status = status_resp["status"]
            if status == 'SUCCESS':
                break
            elif status in good_status:
                print(" "*50, end='\r')
                dots = "." * (count % 4)
                print(status + dots, end="\r")
            else:
                message = status_resp['message']
                raise ValueError("Status: {}. Error during query execution: {}"
                                 .format(status, message))
            count += 1
            time.sleep(1)
        return status_resp

    # def execute_remote(self, dataset, output="tab"):
    #     if not isinstance(dataset, GMQLDataset):
    #         raise TypeError("A GMQLDataset is required. {} was found".format(type(dataset)))
    #
    #     dag = dataset._get_serialized_dag()
    #     self._execute_dag(dag, output)
    #     # TODO: complete...

    def execute_remote_all(self, output="tab", output_path=None):
        pmg = get_python_manager()
        serialized_dag = pmg.get_serialized_materialization_list()
        # with open("/home/luca/Documenti/dag.dag", "w") as f:
        #     f.write(serialized_dag)
        return self._execute_dag(serialized_dag, output, output_path)

    def _execute_dag(self, serialized_dag, output="tab", output_path=None):
        header = self.__check_authentication()
        header['Content-Type'] = "text/plain"
        output = output.lower()
        if output not in ['tab', 'gtf']:
            raise ValueError("output must be 'tab' or 'gtf'")

        url = self.address + "/queries/dag/" + output
        body = serialized_dag
        response = requests.post(url=url, data=body, headers=header)
        if response.status_code != 200:
            raise ValueError("Code {}. {}".format(response.status_code, response.json().get("error")))
        response = response.json()
        jobid = response.get("id")
        self.logger.info("JobId: {}. Waiting for the result".format(jobid))
        status_resp = self._wait_for_result(jobid)

        datasets = status_resp.get("datasets")
        result = []

        if isinstance(output_path, bool):
            if output_path:
                output_path = TempFileManager.get_new_dataset_tmp_folder()
            else:
                output_path = None

        for dataset in datasets:
            name = dataset.get("name")
            if isinstance(output_path, str):
                path = os.path.join(output_path, name)
                result.append(path)
                self.download_dataset(dataset_name=name, local_path=path)

        return result


    """
        Execution
    """

    def trace_job(self, jobId):
        """ Get information about the specified remote job

        :param jobId: the job identifier
        :return: a dictionary with the information
        """
        header = self.__check_authentication()
        status_url = self.address + "/jobs/" + jobId + "/trace"
        status_resp = requests.get(status_url, headers=header)
        if status_resp.status_code != 200:
            raise ValueError("Code {}. {}".format(status_resp.status_code, status_resp.json().get("error")))
        return status_resp.json()


def create_callback(encoder, n_files=None):
    encoder_len = encoder.len
    if n_files is not None:
        tot_len = n_files
        byte_per_file = encoder_len / n_files
    else:
        tot_len = encoder_len

    bar = tqdm(total=tot_len, disable=not is_progress_enabled())

    if n_files is not None:
        def callback(monitor):
            bar.update(max(int((monitor.bytes_read / byte_per_file) - bar.n), 0))

        return callback

    def callback(monitor):
        bar.update(monitor.bytes_read - bar.n)

    return callback
