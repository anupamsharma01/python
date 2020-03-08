import time
import datetime
import string
import os
import sys
import sqlite3
import zipfile
import cboe_weeklysmf
import option_chain_dump
import load_dump_to_table
import Alert_ATM_straddle_Premium

def option_chain_trigger_module():
    # log file handling
    logging_to_file=0
    if (logging_to_file):
        old_stdout = sys.stdout
        log_file = open("C:\Anupam\market\option_chain\option_chain.log", "a")
        sys.stdout = log_file

    #sqlite3 connection
    connection = sqlite3.connect('C:\Anupam\market\option_chain\options_chain_dump.db')
    cursor = connection.cursor()

    select_sql = "SELECT equity FROM CBOE_DATA WHERE product_type in ('Equity', 'ETF')" # and equity like 'F%' LIMIT 10"

    cursor.execute(select_sql)
    rows=cursor.fetchall()
    print ('Number of Equities fetched from cboe_data to trigger batch: ', len(rows))

    start_time = datetime.datetime.now()
    today = datetime.datetime.strftime(start_time, '%Y-%m-%d')
    #today = '2019-09-05' #DEBUG ONLY
    hr = start_time.hour

    if (hr == 10):
        batch = '10AM'
    elif (hr == 11):
        batch = '11AM'
    elif (hr == 12):
        batch = '12PM'
    elif (hr == 13):
        batch = '1PM'
    elif (hr == 14):
        batch = '2PM'
    elif (hr == 15):
        batch = '3PM'
    elif (hr == 16):
        batch = '4PM'
    else:
        batch = 'NA'

    dump_path = 'C:\Anupam\market\option_chain\dumps'
    dump_filenm = 'dump_' + today + '_' + batch + '.csv'
    dump_path_filenm = dump_path + '\\' + dump_filenm
    dump_straddle_path_filenm = dump_path + '\\' + 'dump_atm_straddle_' + today + '_' + batch + '.csv'

    file_exists = os.path.isfile(dump_path_filenm)
    if ( file_exists ):
        print ('option_chain_trigger:', file_exists, ': Error: ', dump_filenm, ' already EXISTS!!! Skipping call to module scripts for batch:', today, '_' ,batch)
        exit()
    print ('option_chain_trigger: Running Batch dump file: ', dump_filenm )


    if (batch == '10AM'):
        print ('option_chain_trigger: Running cboe_weeklysmf at:', start_time)
        cboe_weeklysmf.refresh_weeklysmf()

    print ('option_chain_trigger: Running option_chain_dump module for: ', batch, ':', dump_path_filenm)
    for row in rows:
        equity=row[0].strip();
        #print ('Trigerred:', equity)
        option_chain_dump.option_chain_dump_module(equity,batch,dump_path_filenm)

    if (batch == '4PM'):
        print ('option_chain_trigger: Running load_dump_to_table for:', dump_filenm)
        load_dump_to_table.load_dump_to_table_module(dump_path_filenm)
    else:
        print ('option_chain_trigger: load_dump_to_table skipped for dump_file:', dump_filenm)

    #Zip dump files
    with zipfile.ZipFile(dump_path_filenm + '.zip', 'w') as zip:
        zip.write(dump_path_filenm, dump_filenm, compress_type=zipfile.ZIP_DEFLATED)
    zip.close()

    #RM .csv file logic
    if (os.path.isfile(dump_path_filenm) and os.path.isfile(dump_path_filenm+'.zip')):
        #print("option_chain_trigger: Deleting file:", dump_path_filenm)
        os.remove(dump_path_filenm)
    else:
        print("option_chain_trigger: Not deleting .csv file since either .csv or .zip is missing")
        print('os.path.isfile(dump_path_filenm):', os.path.isfile(dump_path_filenm))
        print('os.path.isfile(dump_path_filenm.zip):', os.path.isfile(dump_path_filenm + '.zip'))

    #Run Straddle Alert
    print ('Triggering Alert_ATM_straddle_Premium script..')
    Alert_ATM_straddle_Premium.Alert_ATM_straddle_Premium_module(batch, dump_straddle_path_filenm)
    #os.system('python C:/Anupam/market/option_chain/Alert_ATM_straddle_Premium.py')
    #done

    #Run Calendar dump
    if (batch == '4PM'):
        print ('Triggering option_chain_get_calendar script..')
        os.system('python C:/Anupam/market/option_chain/option_chain_get_calendar.py')

    end_time = datetime.datetime.now()
    print ('option_chain_trigger: Program execution time was', end_time - start_time)

#Debug Only
#option_chain_trigger_module()
