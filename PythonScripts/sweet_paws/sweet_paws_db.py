# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 18:01:24 2020

@author: theo
"""

import os
import pandas as pd
import drda

HOST = 'localhost'
DB_NAME = r'C:\testDB2'
PORT = 1527
USER = 'theo'
PASSWORD = 'artemis'

conn = drda.connect(HOST, DB_NAME, PORT, user = USER, password = PASSWORD)
cur = conn.cursor()
cur.execute('select * from Customers')
