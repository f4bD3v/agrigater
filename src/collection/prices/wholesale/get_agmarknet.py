#!/usr/bin/env python2

import requests
import re
import sys
import datetime
import os
from time import sleep
from optparse import OptionParser
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from os import path, remove

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
category = ''
fs_category = ''

def download_data(date_string):
    outdir = path.join(data_dir, 'by_date_and_commodity', fs_category)
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    outfile = '_'.join([commodity, re.sub('/', '-', date_string) + '.csv']).replace('/', ',')
    #outfile = re.sub(r'\s+', '', outfile)
    outpath = path.join(outdir, outfile)
    #outpath = outpath.replace('(', '\(').replace(')', '\)')
    print(outpath)
    if path.isfile(outpath):
        #remove(outpath)
        return

    """Download weekly prices in HTML and save them to file CSV"""
    url="http://agmarknet.nic.in/cmm2_home.asp?comm={0}&dt={1}".format(commodity.strip("\""), date_string)
    print(url)
    r = requests.get(url)
    #response = urllib2.urlopen(req)
    #result_html = response.read()
    html_to_csv(outpath, date_string, commodity, r.text)

def html_to_csv(outpath, date_string, commodity, html):
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


    #print "### Output file:", out_file_name
    #outfile = open(outfile_name, "w")
    df = pd.DataFrame(all_rows)
    # if the resulting frame is empty => do not save and move on
    if df.empty:
        return
    df = df.apply(lambda x: x.str.strip(), axis=1)
    # columns: 12 without grade
    ## add new columns
    origin = df[3]
    origin = origin.str.strip()
    origin = origin.str.split('_', expand=True)
    if origin.shape[1] == 1:
        origin=pd.concat([origin, origin], axis=1)
        origin.columns = [0,1]
    origin_state = origin[0].str.strip()
    origin_market = origin[1].str.strip()
    df.drop(3, axis=1, inplace=True)
    # insert columns
    tonnage = df[2]
    df.drop(2, axis=1, inplace=True)
    ### price/kg conversion after removing tonnage
    ### TODO: write method that divides every price series irrespective of digit or not
    if len(df.columns) == 6:
        df[[5,6,7]] = df[[5,6,7]].applymap(lambda x: float(x)/100 if x != 'NR' else x) # applymap(lambda x: float(x)/100 if x.isdigit() else x), isdigit does not work for decimals "985.5" (DID NOT REALIZE DECIMALS WERE ENTERED... IDIOT)
    else:
        print(df.head())
        df[[6,7,8]] = df[[6,7,8]].applymap(lambda x: float(x)/100 if x != 'NR' else x) # applymap(lambda x: float(x)/100 if x.isdigit() else x), isdigit does not work for decimals "985.5" (DID NOT REALIZE DECIMALS WERE ENTERED... IDIOT)
    #tonnage.replace(np.nan, '-', regex=True)
    df.insert(0, 'Date', date_string)
    df.insert(3, 'Commodity', commodity)
    # delete tonnage column
    df.insert(5, 'Tonnage', tonnage)
    df.insert(9, 'OriginState', origin_state)
    df.insert(10, 'OriginMarket', origin_market)
    df.replace('^NR$', '*', regex=True, inplace=True)
    #df['Tonnage'] = df['Tonnage'].replace(np.nan, '*')
    #df['Tonnage'] = df['Tonnage'].str.replace('^$', np.nan)
    df.replace('^$', np.nan, regex=True, inplace=True)
    df = df.fillna(method='ffill')
    df.replace('^\*$', np.nan, regex=True, inplace=True)
    #df.replace('*', np.nan, regex=True, inplace=True)
    df[4] = df[4].replace(',', ';')
    df.insert(3, 'Category', category)
    #df['Tonnage'] = df['Tonnage'].str.replace('^$', '-')
    ### pandas add column at specific index
    ### TODO: change this to pandas column operations!
    df = df.where((pd.notnull(df)), None)
    if len(df.columns) == 12:
        df.columns = ['Date', 'State', 'Market', 'Category', 'Commodity', 'Variety', 'Tonnage', 'Min', 'Max', 'Modal', 'OriginState', 'OriginMarket']
    else:
        df.columns = ['Date', 'State', 'Market', 'Category', 'Commodity', 'Variety', 'Grade', 'Tonnage', 'Min', 'Max', 'Modal', 'OriginState', 'OriginMarket']
    pd.DataFrame.to_csv(df, outpath, header=False, index=False, quotechar='"')
    return
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

    parser.add_option("-f", "--category", action="store", nargs=1, dest="category")

    parser.add_option("-c", "--commodity",
                      action="store", nargs=1, dest="commodity")
    (options, args) = parser.parse_args()

    if options.category:
        category = options.category
        category = re.sub('\s+', ' ', category.strip('"'))
        print(category)
        fs_category = re.sub('\s+', ' ', category.replace(',', ' '))
        fs_category = '_'.join(fs_category.split(' '))
        print(fs_category)

    if not options.commodity:
        usage()
        sys.exit("No commodity given!")
    else:
        commodity = options.commodity
        commodity = commodity.strip('"')
        print(commodity)
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

