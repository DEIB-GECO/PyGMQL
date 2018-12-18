#!/usr/bin/env python

import gmql as gl
import argparse
import getpass
import time


def login():
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
        return rm, address, auth_token, s_type, s_time
    except Exception:
        print("Something went wrong. Try again.")
        exit(-1)


def main():
    parser = argparse.ArgumentParser(description='PyGMQL login parser')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--login", help="insert the credential for the GMQL web service", action="store_true")
    group.add_argument("--logout", help="remove from the configuration file all the information for login",
                       action="store_true")

    args = parser.parse_args()
    if args.login:
        print("----- LOGIN TO GMQL WEB SERVICE -----")
        rm, address, auth_token, s_type, s_time = login()
        sm = gl.get_session_manager()
        sm.add_session(address, auth_token, s_time, s_type)

    if args.logout:
        print("----- LOGOUT FROM GMQL WEB SERVICE -----")
        rm, address, auth_token, s_type, s_time = login()
        sm = gl.get_session_manager()
        sm.remove_session(address)


if __name__ == '__main__':
    main()
