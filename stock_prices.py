#!/usr/bin/python2.7

import urllib2
import MySQLdb
import argparse
import time
import os

os.chdir("/the/containing/directory/")

# Parse the args!
parser = argparse.ArgumentParser()
parser.add_argument("-s", "--summary", help="collect summary information from the day, usually called after the market is closed", action="store_true")
args = parser.parse_args()

# Create format string for periodic use
format_str = '&f=snd1t1ab'
if (args.summary):
  # Change format string to get summary information
  format_str = '&f=snopgh'

# Base URL for REST API request
request_str = 'http://finance.yahoo.com/d/quotes.csv'
stock_symbol_str = '?s='


# Parse through the input file of stock symbols, one per line.
first = True
with open('stocks.in') as f:
  for line in f:  
    line = line.strip()
    if (not first):
      stock_symbol_str += '+'
    else:
      first = False
    stock_symbol_str += line

if first:
  # There were no stocks in 'stocks.in'. Exit.
  sys.exit()

responses = urllib2.urlopen(request_str + stock_symbol_str + format_str)
responses_str = responses.read()

# Connect to the 'finance' database
db = MySQLdb.connect('sql-database-address', 'username', 'password', 'database')

# Set up cursor
cursor = db.cursor()

# Function to execute a SQL command
# No sanitization!
# Ensure that the user that is making these accesses has only the permissions they need.
def execute(sql_command):
  try:
    cursor.execute(sql_command)
    db.commit()
  except:
    db.rollback()

# Parse the response from Yahoo.
response_list = responses_str.strip().split('\n')
for response in response_list:
  array = response.split(',')
  if (not args.summary):
    # The company name is allowed to have commas.
    # This relies on there being 6 elements.
    array[1:-4] = [','.join(array[1:-4])]
    # The data is ticker data
    symbol = array[0]
    name   = array[1]
    # Annoying string formatting issues.
    date   = time.strftime("%Y-%m-%d", time.strptime(array[2].strip('"'), "%m/%d/%Y"))
    time_str   = time.strftime("%H:%M:%S", time.strptime(array[3].strip('"'), "%I:%M%p"))
    ask    = array[4]
    bid    = array[5]
    sql = 'insert into ticker (name, symbol, date, time, ask, bid) values ({},{},"{}","{}",{},{});'.format(name, symbol, date, time_str, ask, bid)
    execute(sql)
  else:
    # The company name is allowed to have commas.
    # This relies on there being 6 elements.
    array[1:-4] = [','.join(array[1:-4])]
    # The data is summary data
    symbol     = array[0]
    name       = array[1]
    open_val   = array[2]
    prev_close = array[3]
    low        = array[4]
    high       = array[5]
    sql = 'insert into daily_summary (name, symbol, date, open, prev_close, high, low) values ({},{},"{}",{},{},{},{});'.format(name, symbol, time.strftime("%Y-%m-%d"), open_val, prev_close, high, low)
    execute(sql)

db.close()


