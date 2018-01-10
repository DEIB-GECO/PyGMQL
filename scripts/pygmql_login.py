#!/usr/bin/env python

import gmql as gl
import argparse
import getpass
import time

parser = argparse.ArgumentParser(description='PyGMQL login parser')
parser.add_argument("--login", help="insert the credential for the GMQL web service", action="store_true")

args = parser.parse_args()

if args.login:
    print("----- LOGIN TO GMQL WEB SERVICE -----")
    address = input("http address of web service: ")
    username = input("username: ")
    password = getpass.getpass("password: ")

    gl.set_remote_address(address)
    try:
        rm = gl.RemoteManager(address=address)
        rm.login(username=username, password=password)
        auth_token = rm.auth_token
        s_type = "authenticated"
        s_time = int(time.time())
    except Exception:
        print("Something went wrong. Try again.")
        exit(-1)

    sm = gl.get_session_manager()
    sm.add_session(address, auth_token, s_time, s_type)





