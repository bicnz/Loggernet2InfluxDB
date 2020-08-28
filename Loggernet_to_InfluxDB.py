#!/usr/bin/env python3

import sys
import json
import time
import pandas as pd
import numpy as np

from influxdb import DataFrameClient
args=sys.argv
if len(args) <= 1:
    print("The arguments should be SiteID, CSV filename and number of rows")
    sys.exit()

# Configuration Parameters.
SITE_ID = args[1]
CSV_FILE_NAME = args[2]
INFLUX_HOST = 'localhost'
INFLUX_PORT = 8086
INFLUX_USER = 'username'
INFLUX_PW = 'password'
INFLUX_DB = 'database'
INFLUX_PROT = 'line'
DEBUG = 0

# Read CSV file, Skip additional header rows, set header for column names, set RECORD column as index, change NAN strings to NaN floats
df = pd.read_csv(CSV_FILE_NAME, skiprows=[0,2,3], header = 0, index_col = ['TIMESTAMP'], na_values=['NAN'])
if len(sys.argv) >= 4: # if rows argument was provided, select last x rows from csv file
    ROWS = int(sys.argv[3])
    df = df.tail(ROWS)
    print ("Scraped ",ROWS," rows")
del df['RECORD'] # delete record column
if 'StationID' in df.columns: # delete StationID and SiteID columns if they exist
   del df['StationID']
if 'SiteID' in df.columns:
   del df ['SiteID']
df = df.replace(to_replace ='^.*Not [Ff]itted.*$', value=0.0, regex = True) # Replace Not Fitted values with 0
df = df.replace([np.inf, -np.inf], np.nan) # replace inf or -inf values with NaN floats
df = df.fillna(0.0) # replace NaN's with 0
float_col = df.select_dtypes(include=['int64']) # Select integer columns and recast to float
for col in float_col.columns.values:
        df[col] = df[col].astype('float64')
mask = df.astype(str).apply(lambda x : x.str.match(r'\d{4}-\d{2}-\d{2} \d{2}\:\d{2}\:\d{2}').all()) # regex for timestamps
df.loc[:,mask] = df.loc[:,mask].apply(pd.to_datetime) # set timestamps to datetime dtype
df.index = pd.DatetimeIndex(df.index).tz_localize('Etc/GMT-12').tz_convert('UTC') # convert timestamp to UTC
time_col = df.select_dtypes(include=['datetime64']) # select other time fields and delete (Max Gust Time etc)
for col in time_col.columns.values:
        del df[col]
df = df.loc[:, (df != 0.0).any(axis=0)] # delete column if it has no values other than 0

if DEBUG == 1:
    print (df.dtypes)
    print (df)

client = DataFrameClient(INFLUX_HOST, INFLUX_PORT, INFLUX_USER, INFLUX_PW, INFLUX_DB)

client.write_points(df, measurement=SITE_ID, protocol=INFLUX_PROT)
