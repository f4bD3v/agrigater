#!/usr/bin/env python
# EXTRACT COMMODITY NAMES

import requests
import re
import os

from BeautifulSoup import BeautifulSoup

url = "http://agmarknet.nic.in/agnew/CommodityList.aspx"
resp = requests.get(url)
html = resp.text

html = re.sub(r'&nbsp;?', '', html)
html = re.sub(r'</?font[^>]*>', '', html)

soup = BeautifulSoup(html)
tables = soup.findAll('table')
table = tables[2]

comm_file = open("commodities.txt", "w")
for row in table.findAll("tr"):
    tds = row.findAll("td")
    if len(tds) == 1:
        family=tds[0].getText()
        comm_file.write("#"+family+"\n")
        row.append(family)
    else:
        for td in tds:
            comm = td.getText()
            if not comm:
                continue
            else:
                comm_file.write(comm+"\n")

comm_file.close()    

