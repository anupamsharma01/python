import quandl

from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import requests
import ssl
import sys
import tdameritrade.auth #added as40183
import urllib
import urllib3 #as40183

from sys import argv
import pymysql.cursors
import datetime
import dateutil.relativedelta
import calendar
import time
import json
import ast
import pandas
import sqlite3

import string
import xlwt
import openpyxl


quandl.ApiConfig.api_key = "SvuFdGGbsmnGvYywj2VZ"

#data = quandl.get("EIA/PET_RWTC_D")

#print (data)

url = "https://www.quandl.com/api/v3/datatables/ZACKS/EA/metadata?api_key=SvuFdGGbsmnGvYywj2VZ"

r = requests.get(url)
payload = r.json()

print (payload)

data = quandl.get_table('ZEA/F1', paginate=True)
#https://www.quandl.com/api/v3/datatables/ZACKS/EA?api_key=SvuFdGGbsmnGvYywj2VZ
