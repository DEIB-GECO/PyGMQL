import pandas as pd

LOCAL = "local"
DELETE_LOCAL = "delete_local"
REMOTE = "remote"
DELETE_REMOTE = "delete_remote"
PARSER = "parser"


class SourcesTable:
    """ Handles the location of the datasets during the program execution.
    Manages both local and remote datasets. It is used when a mode change is
    detected to send or download data sources to/from the remote server.
    """
    def __init__(self):
        self.table = pd.DataFrame(columns=[LOCAL, DELETE_LOCAL,  REMOTE, DELETE_REMOTE, PARSER])
        self.id_count = 0

    def add_source(self, local=None, delete_local=False, remote=None, delete_remote=False, parser=None):
        self.table = self.table.append({LOCAL: local,
                                        DELETE_LOCAL: delete_local,
                                        REMOTE: remote,
                                        DELETE_REMOTE: delete_remote,
                                        PARSER: parser}, ignore_index=True)
        result = self.id_count
        self.id_count += 1
        return result

    def get_source(self, id):
        res = self.table.loc[id].to_dict()
        return res

    def modify_source(self, id, local=None, delete_local=None, remote=None, delete_remote=None):
        if local is not None:
            self.table.loc[id][LOCAL] = local
        if delete_local is not None:
            self.table.loc[id][DELETE_LOCAL] = delete_local
        if remote is not None:
            self.table.loc[id][REMOTE] = remote
        if delete_remote is not None:
            self.table.loc[id][DELETE_REMOTE] = delete_remote

    def search_source(self, local=None, remote=None):
        if local is not None:
            ids = self.table[self.table[LOCAL] == local].index.tolist()
        elif remote is not None:
            ids = self.table[self.table[REMOTE] == remote].index.tolist()
        else:
            raise ValueError("local or remote must be given")
        return ids[0] if len(ids) > 0 else None

    def get_deletable(self, where="remote"):
        if where == 'remote':
            names = self.table.loc[self.table[DELETE_REMOTE], REMOTE].tolist()
        elif where == 'local':
            names = self.table.loc[self.table[DELETE_LOCAL], LOCAL].tolist()
        else:
            raise ValueError("where must be 'remote' or 'local'")
        return names
