import os
import xml.etree.ElementTree as ET
from ..FileManagment.SessionFileManager import get_user_dir
from xml.dom import minidom

SESSIONS_TAG = "sessions"
SESSION_TAG = "session"
ADDRESS_TAG = "address"
LAST_USAGE_TAG = "last_usage"
TOKEN_TAG = "token"
TYPE_TAG = "type"

session_file_path = os.path.join(get_user_dir(), "sessions.xml")


class SessionManager:
    def __init__(self, sessions):
        self.sessions = sessions

    def add_session(self, address, auth_token, time, type):
        self.sessions[address] = (time, auth_token, type)

    def remove_session(self, address):
        self.sessions.pop(address)

    def get_session(self, address=None):
        # if there is no session
        if len(self.sessions) == 0:
            return None
        else:
            if address is None:
                # get last session
                ordered_sessions = sorted(self.sessions.items(), key=lambda x: x[1][0])
                last_session = ordered_sessions[0]
                address = last_session[0]
                auth_token = last_session[1][1]
                s_type = last_session[1][2]
                return address, auth_token, s_type
            else:
                res = self.sessions.get(address)
                if res is None:
                    return None
                else:
                    return address, res[1], res[2]


def load_sessions():
    if not os.path.isfile(session_file_path):
        return SessionManager(dict())
    tree = ET.ElementTree(file=session_file_path)
    sessions = tree.getroot()
    res = {}
    for session_node in sessions.findall(SESSION_TAG):
        sn_address = session_node.find(ADDRESS_TAG).text
        sn_auth_token = session_node.find(TOKEN_TAG).text
        sn_last_usage = int(session_node.find(LAST_USAGE_TAG).text)
        sn_type = session_node.find(TYPE_TAG).text
        res[sn_address] = (sn_last_usage, sn_auth_token, sn_type)

    return SessionManager(res)


def store_sessions(sessions):
    sessions_node = ET.Element(SESSIONS_TAG)
    for address in sessions.keys():
        time, auth_token, s_type = sessions[address]
        session_node = ET.SubElement(sessions_node, SESSION_TAG)

        address_node = ET.SubElement(session_node, ADDRESS_TAG)
        address_node.text = address

        at_node = ET.SubElement(session_node, TOKEN_TAG)
        at_node.text = auth_token

        time_node = ET.SubElement(session_node, LAST_USAGE_TAG)
        time_node.text = str(time)

        type_node = ET.SubElement(session_node, TYPE_TAG)
        type_node.text = s_type

    # tree = ET.ElementTree(sessions_node)
    rough_string = ET.tostring(sessions_node, encoding="utf-8")
    reparsed = minidom.parseString(rough_string)
    pretty_string = reparsed.toprettyxml()
    with open(session_file_path, "w") as f:
        f.write(pretty_string)
