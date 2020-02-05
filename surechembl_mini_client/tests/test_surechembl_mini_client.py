#!/usr/bin/env python

import unittest
import sqlite3
from sqlite3 import Error

from sqlalchemy import create_engine

from surechembl_mini_client import surechembl_mini_client


class surechembl_mini_client_test(unittest.TestCase):

    # create in-memory sqllite DB
    ftp_usr = 'scftp'
    ftp_psw = ''

    # database type to drivername
    conn_info = {
        'drivername' : '',
        'username' : '',
        'password' : '',
        'host' : '',
        'port' : '',
        'database' : 'sqlite://'
    }

    def test_today_frontfile(self):
        surechembl_mini_client(ftp_usr, ftp_psw, conn_info, frontfile=True)

    def test_custom_frontfile(self):
        surechembl_mini_client(ftp_usr, ftp_psw, conn_info, frontfile=True, custom_day=26, custom_month=1, custom_year=2019)

    def test_backfile(self):
        surechembl_mini_client(ftp_usr, ftp_psw, conn_info, frontfile=False, start_year=1950, end_year=1970)

if __name__ == '__main__':

    unittest.main()
