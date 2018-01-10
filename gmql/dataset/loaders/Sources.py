import pandas as pd

LOCAL = "local"
REMOTE = "remote"
PARSER = "parser"


class SourcesTable:
    """ Handles the location of the datasets during the program execution.
    Manages both local and remote datasets. It is used when a mode change is
    detected to send or download data sources to/from the remote server.
    """
    def __init__(self):
        self.table = pd.DataFrame(columns=[LOCAL, REMOTE, PARSER])
        self.id_count = 0

    def add_source(self, local=None, remote=None, parser=None):
        self.table.loc[self.id_count] = {LOCAL: local, REMOTE: remote, PARSER: parser}
        result = self.id_count
        self.id_count += 1
        return result

    def get_source(self, id):
        res = self.table.loc[id].to_dict()
        return res

    def modify_source(self, id, local=None, remote=None):
        if local is not None:
            self.table.loc[id][LOCAL] = local
        if remote is not None:
            self.table.loc[id][REMOTE] = remote

    def search_source(self, local=None, remote=None):
        if local is not None:
            ids = self.table[self.table[LOCAL] == local].index.tolist()
        elif remote is not None:
            ids = self.table[self.table[REMOTE] == remote].index.tolist()
        else:
            raise ValueError("local or remote must be given")
        return ids[0] if len(ids) > 0 else None
