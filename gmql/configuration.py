class Configuration:
    """ Class containing all the information regarding the system environment and the Spark environment
    """

    def __init__(self):
        self.app_name = "gmql_api"
        self.master = "local[*]"
        self.sc = None
        self._properties = dict()
        self._system = dict()

    def get_spark_confs(self):
        return self._properties

    def get_system_confs(self):
        return self._system

    def set_spark_context(self, sc):
        self.sc = sc

    def set_app_name(self, name):
        """ Sets the name of the application in spark,
        By default it is called "gmql_api"

        :param name: string
        :return: None
        """
        self.app_name = name

    def set_master(self, master):
        """ Set the master of the spark cluster
        By default it is "local[*]"

        :param master: string
        :return: None
        """
        self.master = master

    def set_spark_conf(self, key=None, value=None, d=None):
        """ Sets a spark property as a ('key', 'value') pair of using a dictionary
        {'key': 'value', ...}

        :param key: string
        :param value: string
        :param d: dictionary
        :return: None
        """
        if isinstance(d, dict):
            self._properties.update(d)
        elif isinstance(key, str) and isinstance(value, str):
            self._properties[key] = value
        else:
            raise TypeError("key, value must be strings")

    def set_system_conf(self, key=None, value=None, d=None):
        """ Sets a java system property as a ('key', 'value') pair of using a dictionary
        {'key': 'value', ...}

        :param key: string
        :param value: string
        :param d: dictionary
        :return: None
        """
        if isinstance(d, dict):
            self._system.update(d)
        elif isinstance(key, str) and isinstance(value, str):
            self._system[key] = value
        else:
            raise TypeError("key, value must be strings")