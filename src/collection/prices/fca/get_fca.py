#!/usr/bin/env python2

import urllib2
import shutil
import re
import sys
import datetime
import os
from time import sleep
from optparse import OptionParser
from random import randint
from BeautifulSoup import BeautifulSoup

usage_str = """
This scripts downloads daily retail food prices from http://fcainfoweb.nic.in/PMSver2/Reports/Report_Menu_web.aspx. Results are saved in CSV format to csv_out/ directory.

Usage:
    python2 download_fcainfoweb_daily.py [options]

    -d DATE                 -- download data for this date
    -r STARTDATE ENDDATE    -- download all data in this date range
    -f                      -- use local raw HTML files instead of downloading

Examples:

    python2 download_fcainfoweb_daily.py -d 02/03/2012 
    python2 download_fcainfoweb_daily.py -r 20/01/2013 20/03/2013 -f
"""

data_dir = '../../../../data/fca'
csv_out_dir = os.path.join(data_dir, 'csv')
raw_out_dir = os.path.join(data_dir, 'html')
price_type = '' 

# Easiest way to get a new cookie is to visit http://fcainfoweb.nic.in/PMSver2/Reports/Report_Menu_web.aspx with your browser and copy the cookie ASP.NET_SessionId here
cookie = '5gphqba0bmqnptrabduxgpxw'

def download_data(date_string):
    """Download weekly prices in HTML and save them to CSV"""

    main_url = 'http://fcainfoweb.nic.in/PMSver2/Reports/Report_Menu_web.aspx'
    params = 'MainContent_ToolkitScriptManager1_HiddenField=%3B%3BAjaxControlToolkit%2C+Version%3D4.1.51116.0%2C+Culture%3Dneutral%2C+PublicKeyToken%3D28f01b0e84b6d53e%3Aen-US%3Afd384f95-1b49-47cf-9b47-2fa2a921a36a%3A475a4ef5%3Aaddc6819%3A5546a2b%3Ad2e10b12%3Aeffe2a26%3A37e2e5c9%3A5a682656%3Ac7029a2%3Ae9e598a9&__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE=yH5mVQmrsPrtjf0ljI5Mlcd0qdiFcFXNP%2FFPwJci%2FUcYadb4CwyXqpwJZPW5o6p8C5u%2Btl8Wuofd1ynZQU%2BderIgEg0jxnCEJXyanS4%2BQtwBlBfu1RSz321nL4ZDGACGinD2bW74w0j3bHhRchG9lmDJP5VXj2zyBKD0bRFJ1Jub6Ja5bHIRxL0bT9I2sSEvnLr5uP4dMsocZLpzbOSfC%2FLhMFvQSXaC5faZwNJe4ryEO3ZtqbyF3vfx9Q%2FIIZNhnMlvewa595VR6VDJu4OSm%2FC4dAW8vJ2iv1OZkxP1M4HjljO1%2FixAZU045hYvhORQZECgPSAk%2FpX33ej1%2F72SVSWM6DMj%2BO3a6lRaMgwMY9bne1Oz3I%2B%2F1E1A%2B5ApwQ6cCnWs7bc6vXqvdbL27q7g3KV59Kaz%2BRuwM%2FB%2FnklAO0D92DPN7%2FCq7UNbLzmeaIFEHCSXcu5lBwQHc%2BfXFhGYomOP6p%2B2mhaTu68QqysK7HDEGCg3IUYRHin02ZxeHzproDdBN%2Fd6MyPYLr4dOQTOcqTW2zQcrotPvBhOi4xKg3c7bd6bM2sre9JB5LGeuo1G&__VIEWSTATEGENERATOR=85862B00&__VIEWSTATEENCRYPTED=&__EVENTVALIDATION=ahy%2F909LpNo5V6bxAb7MI4VcmAZhQOg2q6R2sYtPOPJp5%2FQ%2Brvy847KU8j5ouhQcuF4oR%2BBPcMY%2FZ8JUwEcxnjTQSID%2F3lqudPBFVYDefcS7nonNpHyfPQJskq5KtsNcX48TuAmkGcIozreudhIsZup15oPGevt6MvN484Upz8UqkA1%2B9Twd%2F4F7K2aA528ZyN8Txk5mP88O2Gr%2FuBpXPf7BkIthp8Lcbpv5AtBFrqN7ePqy7dLgIld%2BOxHKLq%2FI9OStH6DrkIBx7NiipHS2OT5l1DxpFgRj45EB6IdFU8wyHTVWwYW5mKkD1%2BWprwcGvG5mSU6m1iyuRAHTjHyOqbebk2XLiwB534hOnkiwowl7aNrcnOOwUpJ%2Bj3MXd5y3Vcwao6wcV00Tnp9xRMLYbE2yReN8RpvbjC0klpH4LOyAfswV0Orxwn1usohUy%2FRl&ctl00%24MainContent%24Ddl_Rpt_type=#{TXTPRICE}&ctl00%24MainContent%24ddl_Language=English&ctl00%24MainContent%24Rbl_Rpt_type=Price+report&ctl00%24MainContent%24Ddl_Rpt_Option0=Daily+Prices&ctl00%24MainContent%24Txt_FrmDate=#{TXTDATE}&ctl00%24MainContent%24btn_getdata1=Get+Data' 

    params = params.replace('#{TXTDATE}', date_string)
    params = params.replace('#{TXTPRICE}', price_type)
    opener = urllib2.build_opener()
    opener.addheaders.append(('Cookie', 'ASP.NET_SessionId=' + cookie))
    req = opener.open(main_url, params)
    result_html = req.read()
    save_downloaded_data(date_string, result_html)

def save_downloaded_data(date_string, html):
    # Remove some stuff
    html = re.sub(r'<input type="(hidden|submit)"[^>]*>', '', html)
    html = re.sub(r'\s*bordercolor\w*=\w*', '', html)
    html = re.sub(r'\s*align\w*=\w*', '', html)
    html = re.sub(r'<script.+?</script>', '', html, flags=re.DOTALL)

    if html.find('Data does not exist for this date') != -1:
        # No data for the given date
        print ">>> No data for %s" % date_string
        return

    if html.find('Wrong Input or Unauthorised User') != -1:
        # Wrong cookie?
        sys.exit('Session expired, provide a new cookie!')
        return
    # Save raw file
    raw_file_name = get_raw_filename(date_string)
    table = str(html)
    raw_file = open(raw_file_name, "w")
    raw_file.write(table)
    raw_file.close()
    to_csv(date_string, html)
    print "Saved data for %s" % date_string

def get_raw_filename(date_string):
    raw_file_name = raw_out_dir + '/' + price_type + '_' + re.sub('/', '-', date_string) + '.html'
    return raw_file_name

def to_csv_from_file(date_string):
    raw_filename = get_raw_filename(date_string)
    if not os.path.exists(raw_filename):
        print '>>> No file:', raw_filename
        return
    with open(raw_filename, 'r') as f:
        html = f.read()
        to_csv(date_string, html)

def process_name(name):
    sub_dict = {'T.PURAM': 'Thiruvananthapuram'}
    if name in sub_dict:
        name = sub_dict[name]
    name = name.capitalize()
    return name

def extract_tables(centers):
    center = centers[1]
    tables = []
    while True:
        table = center.findChildren('table')[0]
        tables.append(table)
        #yield table
        centers = center.findChildren('center')
        if not centers:
            break
        center = centers[2]
    return tables

def to_csv(date_string, html):
    soup = BeautifulSoup(html)

    # recursive table extraction
    tables = extract_tables(soup.findAll('center'))
    print len(tables)
    if len(tables) < 3:
        return

    out_file_name = csv_out_dir + '/' + price_type + '_' + re.sub('/', '-', date_string) + '.csv'
    print "### Output file:", out_file_name
    out_file = open(out_file_name, "w")

    # parsing 
    rows = []
    for t in tables:
        rows = t.findAll('tr')
        header_row = rows[0]
        headers = header_row.findAll('td')
        products = []
        for product_header in headers[1:]:
            header_text = product_header.getText()
            header_text = re.sub(r'/\s*', '/', header_text)
            header_text = re.sub(r'-\s*|\s*[*@]', '', header_text)
            products.append(header_text)
        nproducts = len(products)
        print products

        data_rows = rows[1:]
        zone = ''
        for row in data_rows:
            cells = row.findAll('td')
            if not cells:
                continue
            if len(cells) < nproducts:
                zone=cells[0].getText().capitalize()
                continue
            city_name = cells[0].getText()
            if re.search(r'(Maximum|Minimum|Modal) Price', city_name):
                # ignore final rows
                continue
            city_name = process_name(city_name)
            cells = cells[1:]
            for i in xrange(nproducts):
                product = products[i]
                price = cells[i].getText()
                if 'NR' in price:
                    price = '*'
                csv_row = ','.join([date_string, zone, city_name, product, price])+'\n'
                out_file.write(csv_row)

    out_file.close()

def download_range(drange, from_file):
    srange, erange = drange
    sdate = validate_date(srange)
    edate = validate_date(erange)
    
    if sdate > edate:
        sys.exit("ERROR: start date > end date")

    curdate = sdate
    while curdate <= edate:
        date_string = curdate.strftime("%d/%m/%Y")
        print date_string
        if from_file:
            to_csv_from_file(date_string)
        else:
            print 'downloading'
            download_data(date_string)
        curdate += datetime.timedelta(days=1)

def validate_date(date_string):
    match = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_string)
    if not match:
        sys.exit("ERROR: invalid date, " + date_string)
    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    date = datetime.date(year, month, day)
    return date

def check_out_dir():
    if not os.path.exists(csv_out_dir):
        os.makedirs(csv_out_dir)
    if not os.path.exists(raw_out_dir):
        os.makedirs(raw_out_dir)

def usage():
    print usage_str

def usage_callback(option, opt, value, parser):
    usage()
    sys.exit(1)

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    parser = OptionParser(add_help_option=False)
    parser.add_option("-h", "--help",
                      action="callback", callback=usage_callback)

    parser.add_option("-p", "--pricetype",
                        action="store", nargs=1, dest="price_type")

    parser.add_option("-r", "--range",
                      action="store", nargs=2, dest="drange")

    parser.add_option("-d", "--date",
                      action="store", nargs=1, dest="date")

    parser.add_option("-f", "--from-file",
                      action="store_true", dest="from_file")

    (options, args) = parser.parse_args()

    if options.price_type:
        price_type = options.price_type

    check_out_dir()
    if options.date:
        date_str = options.date
        date = validate_date(date_str)
        download_data(date_str)
    elif options.drange:
        download_range(options.drange, options.from_file)
    else:
        usage()
        sys.exit(1)
    print "### Finished."

