from __future__ import print_function
"""
    @author: Fabian Brix
    .py script to consolidate data from different sources (agmarknet, fca, shapefile administrational data)

"""
import glob
from os import path
#import sys
import csv
import pickle
import logging

# python 2.x to make print assignment work

import cleaner
import retrieval_by_pin

data_dir = '../../data'
admin_dict = {}

corrections_dict = {}
andhra = { 'W Godavary': 'West Godavari',
          'Ananthapuramu': 'Ananthapur',
          'Y S R Kadapa': 'Cuddapah',
          'Visakha ': 'Visakhaptnam ' }

bihar = { 'E Champaran' : 'East Champaran',
         'Purnea' : 'Purnia' }

up = { 'Badaun' : 'Budaun',
        'Divai' : 'Dibai',
        'Dist Bali' : 'Dist Ballia',
        'Choubepur Kanpur' : 'Chaubepur Kanpur Nagar',
      'Rura , Dist Kanpur' : 'Rura, Dist Kanpur Dehat',
      'Kanpur City' : 'Kanpur Nagar',
      'Bullandshar' : 'Bulandshahr' }

punjab = { 'Nawan Shahar' : 'Nawanshahr',
        'Nawanshar' : 'Nawanshahr',
          'Nawanshahar': 'Nawanshahr'}

chattisgarh = { 'Korea' : 'Koriya' }

gujarat = { 'Junagarh' : 'Junagadh' }

corrections_dict['Andhra Pradesh'] = andhra
corrections_dict['Bihar'] = bihar
corrections_dict['Gujarat'] = gujarat
corrections_dict['Chattisgarh'] = chattisgarh
corrections_dict['Uttar Pradesh'] = up
corrections_dict['Punjab'] = punjab

# add pin codes: http://www.pincodein.com/City-Taluka-pin-code/Ahmedabad/11.aspx?StateId=11&District=Ahmedabad&Taluk=City+Taluka
# or use present pin codes to infer (taluk &) district

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
logger.addHandler(logging.FileHandler('test.log', 'a'))
print = logger.info

# extracted from geojson
def create_admin_dict():
    admin = path.join(data_dir, 'state_district_taluk.csv')
    with open(admin, 'r') as admin_csv:
        admin_reader = csv.reader(admin_csv)
        for row in admin_reader:
            (state, district, taluk) = row
            if 'n.a.' in taluk:
               #match = re.search(r'[0-9]+', taluk)
               #taluk = match.group(0)
               continue
            if state in admin_dict:
                if district in admin_dict[state]:
                    if taluk not in admin_dict[state][district]:
                        admin_dict[state][district] = admin_dict[state][district] + [taluk]
                else:
                    admin_dict[state][district] = []
            else:
                admin_dict[state] = {}

    pickle.dump(admin_dict, open('geojson_admin_dict.p', 'wb'))

# extracted from postcodes file
def load_admin_dict(fn):
    admin_dict = pickle.load(open(fn, 'rb'))
    return admin_dict

def main():
    #create_admin_dict()
    admin_dict = load_admin_dict('admin_hierarchy_dict.p')
    taluk_dict = load_admin_dict('dist_by_taluk_dict.p')
    # collect misspellings in file? gather all misspellings of
    # districts in file automatically and then filter manually
    markets = path.join(data_dir, 'markets')
    for name in glob.glob(markets+'/*.csv'):
        admin_states = admin_dict.keys()
        with open(name, 'r') as statemarkets_csv:
            csv_reader = csv.reader(statemarkets_csv)
            header = True

            for row in csv_reader:
                if not header:
                    state = row[1]
                    state = state.replace('Orissa', 'Odisha')
                    state = state.replace('NCT of', '').strip()
                    market = row[0]
                    for astate in admin_states:
                        if astate in state and astate != state:
                            print('%s %s substring matched, but not identical'%(astate, state))
                        #if astate != state:
                        #    print astate, state, ' are not identical'
                        if astate == state:
                            if not astate in corrections_dict:
                                corrections_dict[astate] = {}
                            print('%s %s'%(astate, state))
                            initials = {}
                            for gdist in admin_dict[astate]:
                                initials[gdist] = cleaner.to_initials(gdist)

                            # unnecessarily repeated computation
                            districts = admin_dict[astate].keys()
                            all_state_taluks = reduce(lambda x,y: x|y, admin_dict[astate].values())
                            taluks = list(set(all_state_taluks) - set(districts))

                            print('********************************************************************')
                            print(market)
                            apmc_address = row[3]
                            sec_address = row[4]
                            pin = cleaner.find_pin(apmc_address)# + cleaner.find_pin(sec_address))
                            print(pin)
                            apmc_address = cleaner.normalize(apmc_address, admin_states)
                            sec_address = cleaner.normalize(sec_address, admin_states)
                            address = retrieval_by_pin.get_address_by_pin(pin)
                            print(address)
                            (pstate, dist, taluk) = address

                            print(apmc_address)
                            print(sec_address)

                            if not dist:
                                dist1 = cleaner.assign_district(admin_dict[astate].keys(), apmc_address, initials, corrections_dict[astate])
                                dist2 = set()
                                print(dist1)
                                if not dist1:
                                    dist2 = cleaner.assign_district(admin_dict[astate].keys(), sec_address, initials, corrections_dict[astate])
                                    print(dist2)
                                dist = dist1|dist2
                                print(dist)

                            if not taluk or taluk == '*':
                                taluk_list = all_state_taluks
                                if dist:
                                    if isinstance(dist, set):
                                        d = dist.pop()
                                        taluk_list = admin_dict[astate][d]
                                        dist.add(d)
                                    else:
                                        taluk_list = admin_dict[astate][dist]
                                        dist = set([dist])
                                print(dist)
                                # need to pass by value
                                taluk1 = cleaner.assign_taluk(taluk_list, apmc_address, market)
                                print(dist)
                                taluk2 = set()
                                if not taluk1:
                                    taluk2 = cleaner.assign_taluk(taluk_list, sec_address)
                                taluk = taluk1|taluk2
                                if len(taluk) > 1:
                                    print(taluk)
                                    print('taluk ambiguous!')

                                if not dist:
                                    print(dist)
                                    print('not dist')

                                if not dist and taluk:
                                    t = taluk.pop()
                                    taluk.add(t)
                                    dist = taluk_dict[astate][t]
                                    print(dist)
                                    # address validator to remove wrong matches
                                    if not dist:
                                        print(taluk)
                                        print('could not find district!')
                                    print('%s %s'%(dist, taluk))

                            if isinstance(dist, set) and len(dist) > 1:
                                print('district inference ambiguous!')
                                print(dist)

                            address = (astate, dist, taluk, pin)
                            if astate != state:
                                print('%s %s'%(astate, state))
                                print('Market not from state that was scraped!!!'.upper())
                            print(address)
                            print('********************************************************************')
                            print('\n')

                header = False

if __name__ == "__main__":
    main()
