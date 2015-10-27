import requests
import lxml.html
import re
import csv

def na_replace(text):
    if not text or text == 'NA':
        text = '*'
    return text

def get_staff(html, staff_type):
    prev = get_td_tags_by_txt(html, 'td', staff_type)
    print(staff_type)
    if prev:
        sup = prev[0].getnext()
        supervisory = sup.text_content().strip()
        admin = sup.getnext().text_content().strip()
        if supervisory != '*' and admin != '*':
            staff = str(float(na_replace(supervisory)) + float(na_replace(admin)))
        else:
            staff = '*'
    else:
        staff = '*'
    return staff

def get_cell_content(html, text, indicator=''):
    print(text)
    prev = get_td_tags_by_txt(html, 'td', text)
    val = ''
    if prev:
        print(prev)
        elem = None
        for p in prev:
            if p.getnext() is not None:
                elem = p
                break
        if elem is not None:
            print(elem)
            cell_text = elem.getnext().text_content().strip()
            val = na_replace(cell_text)
            if indicator: # check if string not empty (e.g. 'Regulated')
                if indicator in cell_text:
                    val = str(True)
                else:
                    val = str(False)
        else:
            val = '*'
    else:
        val = '*'
    print(val)
    return val

def get_td_tags_by_txt(html, tag, text):
    reg = [td for td in html.cssselect(tag) if text in td.text_content()]
    return reg

csv_out = '../../../data/markets/'

def main():
    # scrape market site:
    markets = 'http://agmarknet.nic.in/profile/profile_online/combo10.asp'

    r = requests.get(markets)
    source = r.text # r.content - access response body as bytes

    html = lxml.html.fromstring(source)
    links = html.cssselect('a')#xpath('//a') #/@href
    states = []
    for link in links:
        table = link.getparent().getparent().getparent()
        state = table.getprevious().text_content().strip()
        states.append(state)
        state_file = open(csv_out+state+'_markets.csv', 'w') # append to file
        state_file.close()

    market_names = html.xpath('//a/font/text()')
    print(market_names)

    write_header = True
    old_state = ''
    for i in range(len(links)):
        link = links[i]
        state = states[i]
        if state != old_state:
            write_header = True
        market_name = market_names[i]
        state_file = open(csv_out+state+'_markets.csv', 'a') # append to file
        writer = csv.writer(state_file, doublequote=True, quoting=csv.QUOTE_NONNUMERIC)
        print(market_name)
        r = requests.get('http://agmarknet.nic.in/profile/profile_online/'+link.get('href'))
        source = r.text

        html = lxml.html.fromstring(source)
        # extracting tables systematically not useful, directly target cells:
        city = get_cell_content(html, 'City')

        #regulated = html.xpath('//td[text()="Regulated/Unregulated")]/following-sibling')
        regulated = get_cell_content(html, 'Regulated/Unregulated', 'Regulated')

        area_served = get_cell_content(html, 'Geographical area')

        # process later
        holidays = get_cell_content(html, 'Market holidays')
        market_hours = get_cell_content(html, 'Market hours')

        apmc_address = get_cell_content(html, 'Full Postal Address')
        print(apmc_address)
        #print html.xpath('//td//[text()="Full Postal Address"]/following-sibling/text()')
        secretary_address = get_cell_content(html, 'Address of Secretary')
        #print html.xpath('//td[text()="Address of Secretary"]/following-sibling/text()')

        perm_staff = get_staff(html, 'Permanent')
        temp_staff = get_staff(html, 'Temporary')
        # keep full addres for now, do processing with regex later
        # window = rgx_match_window(address)
        # sec_window = rgx_match_window(sec_address)
        # LATER: keep district name that is contained in at least one of the windows

        # year of establishment
        established = get_cell_content(html, 'Year of establishment')

        railway_distance = get_cell_content(html, 'Distance of the railway station')

        transport_incoming = get_cell_content(html, 'Modes of transport generally adopted for the market')
        transport_outgoing = get_cell_content(html, 'Modes of transport for despatches')

        cleaned_graded = get_cell_content(html, 'Whether produce is cleaned/graded before sale', 'Yes')

        nb_cold_storage = get_cell_content(html, 'Number of Cold Storages available')

        nb_commodities_regulated = get_cell_content(html, 'Number of Commodities notified under regulation')

        # leave unprocessed --> get a look at data before further cleaning
        #notified_area = get_cell_content(html, 'Notified area of Market Committee')

        # description of sale: open auction etc., keep as text field, can do further cleaning in R
        sale = get_cell_content(html, 'System of sale')
        payment = get_cell_content(html, 'System of payment')

        # percentages
        commission = get_cell_content(html, 'Commission')
        # do more processing later
        market_fee = get_cell_content(html, 'Market Fee')

        # nominal
        income = get_cell_content(html, 'Annual Income')
        expenditure = get_cell_content(html, 'Annual Expenditure')
        profit = '*'
        if income != '*' and expenditure != '*':
            profit = str(float(income) - float(expenditure))

        reserves = get_cell_content(html, 'Reserves')
        liabilities = get_cell_content(html, 'Liabilities')

        header_1 = 'Market, City, State, Regulated, APMC Address, Secretary Address, Established (Year), Area Served/Catchment Area, Holidays, Hours, Permanent Staff, Temporary staff, Incoming Transport, Outgoging Transport, Railway Distance (km)'
        row_1 = [market_name, city, state, regulated, apmc_address, secretary_address, established, area_served, holidays, market_hours, perm_staff, temp_staff, transport_incoming, transport_outgoing, railway_distance]
        header_2 = '# Regulated Commodities, Cleaning/Grading, # Cold Storage Facilities, Sale Process, Payment Process, Commission, Market Fee, Market Income, Market Expenditure, Profit, APMC Reserves, APMC liabilities'
        row_2 = [nb_commodities_regulated, cleaned_graded, nb_cold_storage, sale, payment, commission, market_fee, income, expenditure, reserves, profit, liabilities]
        #row = map(lambda x: unicode(re.sub('\s+', ' ', x)).encode("utf-8"), row_1+row_2)
        row = map(lambda x: re.sub('\s+', ' ', x), row_1+row_2)
        print(row)
        if write_header:
            header = header_1+', '+header_2
            writer.writerow(tuple(header.split(", ")))
        writer.writerow(tuple(row))

        """
        TODO
            - pop served
            - total annual arrivals
            - nb. of license holders
        """
        write_header = False
        print(str(i)+'/'+str(len(links))+' processed')
        # find next td
        # print tables
        old_state = state
    state_file.close()

if __name__ == '__main__':
    main()







