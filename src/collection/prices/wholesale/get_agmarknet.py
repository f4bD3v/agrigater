#!/usr/bin/env python2

import requests
import re
import sys
import datetime
import os
from time import sleep
from optparse import OptionParser
from random import randint
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import numpy as np
from os import path

usage_str = """
This scripts downloads daily food prices from http://agmarknet.nic.in/cmm2_home.asp for a given commodity. Results are saved in CSV format to csv_out/ directory.

Usage:
    python2 download_agmarket_daily.py [options]

    -d DATE                 -- download data for this date
    -r STARTDATE ENDDATE    -- download all data in this date range
    -c COMMODITY            -- specify commodity (check at the website!)

Examples:

    python2 get_agmarknet.py -d 02/03/2012 -c Rice
    python2 get_agmarknet.py -r 20/01/2013 20/03/2013 -c Tea
"""

data_dir = '../../../../data/agmarknet'

commodity = ''
family = ''


def download_data(date_string):
    """Download weekly prices in HTML and save them to file CSV"""
    url = "http://agmarknet.nic.in/cmm2_home.asp?comm=%s&dt=%s" % (commodity, date_string)
    print(url)
    r = requests.get(url)
    print(r.text)
    #response = urllib2.urlopen(req)
    #result_html = response.read()
    html_to_csv(date_string, commodity, r.text)

def html_to_csv(date_string, commodity, html):
    # Remove some stuff
    html = re.sub(r'&nbsp;?', '', html)
    html = re.sub(r'</?font[^>]*>', '', html)
    soup = BeautifulSoup(html)
    tables = soup.findAll('table')
    if len(tables) < 4:
        # do not exit here, there may be data for other dates
        # sys.exit("ERROR: invalid commodity or no data")
        return
    else:
        table = tables[3]

    all_rows = []
    prev_city = ''
    cell_count = 0
    state = ''
    for row in table.findAll("tr"):
        cur_row = []
        cell_count = len(row.findAll("td"))
        if cell_count == 1:
            state = row.findAll("td")[0].getText()
            continue

        for td in row.findAll("td"):   
            text = td.getText()
            cur_row.append(text)
        if len(cur_row) < 7: continue
        if cur_row[0] == 'Market': continue
        if cur_row[0] == '':
            cur_row[0] = prev_city
        else:
            prev_city = cur_row[0]
        cur_row[:0] = [state]
        cur_row = map(lambda s: re.sub(',', '_', s), cur_row)
        all_rows.append(cur_row)

    #outfile_name = csv_out_dir + '/' + commodity  + '_' \
    #                + re.sub('/', '_', date_string) + '.csv'
    #raw_file_name = raw_out_dir + '/' + commodity  + '_' \
    #                + re.sub('/', '_', date_string) + '.html'
    outdir = path.join(data_dir, 'by_date_and_commodity', family)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile = '_'.join([commodity, re.sub('/', '-', date_string) + '.csv'])
    outpath = path.join(outdir, outfile)

    #print "### Output file:", out_file_name
    #outfile = open(outfile_name, "w")
    df = pd.DataFrame(all_rows) 
    df = df.apply(lambda x: x.str.strip(), axis=1)
    print(df)
    ## add new columns
    df.drop(3, axis=1, inplace=True)
    # insert columns
    tonnage = df[2]
    df.drop(2, axis=1, inplace=True)
    ### price/kg conversion after removing tonnage
    df = df.applymap(lambda x: float(x)/100 if x.isdigit() else x)
    #tonnage.replace(np.nan, '-', regex=True)
    df.insert(0, 'Date', date_string)
    df.insert(3, 'Commodity', commodity)
    # delete tonnage column
    df.insert(5, 'Tonnage', tonnage)
    df.replace('^NR$', '*', regex=True, inplace=True)
    #df['Tonnage'] = df['Tonnage'].replace(np.nan, '*')
    #df['Tonnage'] = df['Tonnage'].str.replace('^$', np.nan)
    df.replace('^$', np.nan, regex=True, inplace=True)
    df = df.fillna(method='ffill')
    df.replace('^\*$', np.nan, regex=True, inplace=True)
    #df.replace('*', np.nan, regex=True, inplace=True)
    df[4] = df[4].replace(',', ';')
    print(df.head())
    print(df.tail())
    #df['Tonnage'] = df['Tonnage'].str.replace('^$', '-')
    ### pandas add column at specific index
    ### TODO: change this to pandas column operations!
    #print(df.head())
    pd.DataFrame.to_csv(df, outpath, header=False, index=False, quotechar='"')
    """
    for r in all_rows:
        # clean out NR values
        del r[3]
        r = map(lambda x: "*" if x == "NR" else x, r)
        tonnes = r[2] #float('0' + re.sub(r'[^\d\.]', '', r[2]))
        # price per kg 
        r = map(lambda x: float(x)/100 if x.isdigit() else x, r)

        r[:0] = [date_string]
        r[3:3] = [commodity]
        del r[4]
        if not tonnes:
            tonnes = "-"
        r[5:5] = [tonnes]
        print r
        row_string = ", ".join([str(c) for c in r]) + "\n"

        # Use modal value
        #row_string = "{}, {}, {}, {}, {}, {}, {}, {}, {}".format(date_string, r[1], r[0], commodity, r[4], float(r[-3])/100, float(r[-2])/100, float(r[-1])/100, tonnes)

        outfile.write(row_string)
    """
    #outfile.close()

    # Save raw file
    #table = str(table)
    #raw_file = open(raw_file_name, "w")
    #raw_file.write(table)
    #raw_file.close()

def download_range(drange):
    srange, erange = drange
    sdate = validate_date(srange)
    edate = validate_date(erange)
    
    if sdate > edate:
        sys.exit("ERROR: start date > end date")

    curdate = sdate
    while curdate <= edate:
        print(curdate)
        download_data(curdate.strftime("%d/%m/%Y"))
        curdate += datetime.timedelta(days=1)
        #sleep(randint(1,3))

def validate_date(date_string):
    match = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_string)
    if not match:
        sys.exit("ERROR: invalid date, " + date_string)
    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    date = datetime.date(year, month, day)
    return date

def usage():
    print(usage_str)

def usage_callback(option, opt, value, parser):
    usage()
    sys.exit(1)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    parser = OptionParser(add_help_option=False)
    parser.add_option("-h", "--help",
                      action="callback", callback=usage_callback)

    parser.add_option("-r", "--range",
                      action="store", nargs=2, dest="drange")

    parser.add_option("-d", "--date",
                      action="store", nargs=1, dest="date")

    parser.add_option("-f", "--family", action="store", nargs=1, dest="family")

    parser.add_option("-c", "--commodity",
                      action="store", nargs=1, dest="commodity")
    
    (options, args) = parser.parse_args()

    if options.family:
        family = options.family

    if not options.commodity:
        usage()
        sys.exit("No commodity given!")
    else:
        commodity = options.commodity
    if options.date:
        date_str = options.date
        date = validate_date(date_str)
        download_data(date_str)
    elif options.drange:
        download_range(options.drange)
    else:
        usage()
        sys.exit(1)
    print("### Finished.")

