#!/usr/bin/env python

import re
import urlparse
import mechanize
import cookielib
import logging
import sys

from bs4 import BeautifulSoup

class CSVScraper(object):
    def __init__(self, url):
        self.url = url
        self.br = mechanize.Browser()
        cj = cookielib.LWPCookieJar()
        self.br.set_cookiejar(cj)
        self.br.set_handle_equiv(True)
        self.br.set_handle_redirect(True)
        self.br.set_handle_referer(True)
        self.br.set_handle_robots(False)
        self.br.addheaders = [('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36')]
        # Log information about HTTP redirects and Refreshes.
        self.br.set_debug_redirects(True)
        # Log HTTP response bodies (ie. the HTML, most of the time).
        self.br.set_debug_responses(True)
        # Print HTTP headers.
        self.br.set_debug_http(True)

        # To make sure you're seeing all debug output:
        self.logger = logging.getLogger("mechanize")
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.logger.setLevel(logging.INFO)

    def submit_form(self, form_id):
        self.br.open(self.url)

        """
        html = BeautifulSoup(self.br.response().read())
        saved_form = html.find('form', id=name).prettify()
        """

        # assume targeted form is nr=0
        self.br.select_form(nr=0)

        self.br.form['ctl00$MainContent$Rbl_Rpt_type'] = ['Price report']

        # create controls
        # select
        self.br.form.new_control('select', 'ctl00$MainContent$Ddl_Rpt_Option0', attrs={'__select': {'id': 'MainContent_Ddl_Rpt_Option0'}})
        self.br.form.new_control('select', 'ctl00$MainContent$Ddl_Rpt_Option0', attrs={'__select': {'id': 'MainContent_Ddl_Rpt_Option0'}, 'value':'Daily Prices', 'selected':'selected'}, index=1)
        self.br.form.new_control('select', 'ctl00$MainContent$Ddl_Rpt_Option0', attrs={'__select': {'id': 'MainContent_Ddl_Rpt_Option0'}, 'value':'Maximum/Minimum Prices'}, index=2)

        # date, submit
        self.br.form.new_control('text', 'ctl00$MainContent$Txt_FrmDate', {'value':''})
        self.br.form.new_control('submit', 'ctl00$MainContent$btn_getdata1', {'value': 'Get Data'})
        self.br.form.fixup()

        date_string = '10/09/2014'

        # ajax public key token
        self.br.form.find_control('MainContent_ToolkitScriptManager1_HiddenField').readonly = False
        self.br.form['MainContent_ToolkitScriptManager1_HiddenField']=';;AjaxControlToolkit, Version=4.1.51116.0, Culture=neutral, PublicKeyToken=28f01b0e84b6d53e:en-US:fd384f95-1b49-47cf-9b47-2fa2a921a36a:475a4ef5:addc6819:5546a2b:d2e10b12:effe2a26:37e2e5c9:5a682656:c7029a2:e9e598a9' 
        self.br.form.find_control('MainContent_ToolkitScriptManager1_HiddenField').readonly = True 
        # assign wanted date
        self.br.form['ctl00$MainContent$Txt_FrmDate'] = date_string

        self.br.form.find_control('__VIEWSTATEGENERATOR').readonly = False

        self.br.form.find_control('__VIEWSTATE').readonly = False
        self.br.form['__VIEWSTATE'] = 'SWPWBK7VrBX7vMod4kf7pwDfSVsmyIQAMjEjw7nDaY%2BsuY9rJ65h5JUNZZP29UdMF19CfKgPUd2t43XLTSI7HdoogCDrPp9AxZmDlnOsXKqn4KAxcB9aXlUtl%2FAIm8NakqPtjRE1r28y%2BPtPTEWI4BFWWRt0LZTwsrGWqc3zglB8mumQtr9EpYysrHLU%2B12d%2BN%2BmABXirJxypY0b6q8UVyxwgb5%2FGUgbnKE0SrkXrAFnAv6zWqMxm7Inqqw2pKb1VbmtYiDrtP45HU9hbY3VXCjt1Ogbi67JXucew2TQoCiLu2UvvhPwK%2F%2BILqGDE%2BbtglvNBjIg9f4yVSQo23vkS7N73WNjy2yLasP8GQYrXSnSddd%2FckUandiFIHri34Yne7FILEA6LfyoTcaKyfOdenVMt6DJk4HokSJ8TZANKVzZ2HqD2z74P%2F%2BHi4nj3fKQ71KkafYbhv7dbWVst2U8SBKXH91LJHr%2FwaWoMbI0mXLEc8l2lPpgMQYpVT9ssfXTim6RqlKQe7NbtSdq0dBQuUZ7FVMtRwAlw96iXTLpIbMQGBgcZUQU3ghGEuOndRgy'
        self.br.form.find_control('__VIEWSTATE').readonly = True
        self.br.form.find_control('__EVENTVALIDATION').readonly = False
        self.br.form['__EVENTVALIDATION'] = 'SozagnzLZ9JQ68Q7y46WHDe2Sw1QwxR3NpNMbv1YoUZGh7MaL5sOkv%2BMt69BhTwRwBB1cVz47RSAoULkCYnELnBrXtwyY%2Fwme6h0g3hfuPtVuZ2oe4lOZrS4Edl%2F%2FmATfV0Ga%2Bx0qPahyTVV4xTLhaYqv9VNI0OTfneUyZDaciG8PwmGUwbumxP7IqY%2BtV2Cz90Gp%2BA2uigq2IE4cUYjWMG3%2FGVjeywC5uUPLOrDEDupPRAH2OOfnJHwVCHJZu5nxl%2FyqXTrn31%2Ff1UPi45o3SmSVomxLLSyC3vgMls0qlNJZQudeKfSf%2BCVDTi6A4TrnDfaEpj%2FXpwt%2BC0USRFpPEx921KQLsqT%2BbTl5H9cTNu9KU%2FP739A1gWF%2BoZQdYOb3FR2f3w%2BxiXMDOITMHbGBZgweC4Cts0kT%2BOXYFSO9tng88UV4cnuTUPpVnYtU3xv'
        self.br.form.find_control('__EVENTVALIDATION').readonly = True


if __name__ == '__main__':
    """
    parser = OptionParser(add_help_option=False)
    parser.add_option("-h", "--help",
                      action="callback", callback=usage_callback)

    parser.add_option("-r", "--range",
                      action="store", nargs=2, dest="drange")

    parser.add_option("-d", "--date",
                      action="store", nargs=1, dest="date")

    parser.add_option("-c", "--commodity",
                      action="store", nargs=1, dest="commodity")
    
    (options, args) = parser.parse_args()
    """

    url = 'http://fcainfoweb.nic.in/PMSver2/Reports/Report_Menu_web.aspx'
    scraper = CSVScraper(url)
    scraper.submit_form('ctl01')
    print scraper.br.form
    print scraper.br._ua_handlers['_cookies'].cookiejar

    # returns a mechanize.Request object
    #request = scraper.br.form.click()
    #response = mechanize.urlopen(request)
    scraper.br.submit()


