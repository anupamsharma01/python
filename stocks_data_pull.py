from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs
import requests
import ssl
import sys
import tdameritrade.auth #added as40183
import urllib
import urllib3 #as40183

from sys import argv
#import option_library
import pymysql.cursors
from datetime import datetime
import time
import math

KEY = 'STOCKTIPS'
# Arguements
#script, equity = argv
in_file = r'C:\Anupam\market\stock_options_api-master\trading_api\tdameritrade\my_programs\data\program_in.txt'
out_file=r'C:\Anupam\market\stock_options_api-master\trading_api\tdameritrade\my_programs\data\program_out.txt'
script='C:/Users/anupshar/.PyCharmCE2017.2/config/scratches/scratch_6.py'


f_in = open(in_file)
equity_list = f_in.readlines()
equity_list = [l.replace('\n','') for l in equity_list]

f_out = open(out_file,'w')
print ('EQUITY | CMP | 52WkRange', file=f_out)


#debug
print ('script=',script)
#print ('equity=',equity)
# Declare
start = datetime.now()
args_list = []
count = str(250)
myFormat = "%Y-%m-%d"
today = datetime.now()
start_date = today.strftime(myFormat)
#equity_list = []


# for stock in equity_list:

for equity in equity_list:
    #equity= equity_list[0]
    start_equity = datetime.now()
    print(equity)
    url = 'https://api.tdameritrade.com/v1/marketdata/'+equity+'/quotes?apikey='+KEY
    #url= 'http://google.com'
    #url1 = 'https://api.tdameritrade.com/v1/marketdata/AAPL/quotes?apikey=STOCKTIPS'
    print(url)
    r = requests.get(url)
    #debug
    print ('r=',r)
    print ('r.text=',r.text)
    payload = r.json()

    print ('payload=',payload)
    table = equity + str("_options")
    equity = payload[equity]['symbol']
    cmp = payload[equity]['lastPrice']
    _52WkLow = payload[equity]['52WkLow']
    _52WkHigh = payload[equity]['52WkHigh']
    print ('table=',table)
    print ('equity=',equity)
    print ('cmp=',cmp)
    #with open(file, 'a') as the_file:     the_file.write('Hello\n')

#    print ('EQUITY | CMP | 52WkRange', file=f)
    #print (equity, '|', cmp, '|', _52WkLow, ' - ', _52WkHigh, file=f)
    print (equity, '|', cmp, '|', _52WkLow, '-', _52WkHigh, file=f_out)

