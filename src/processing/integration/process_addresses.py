from __future__ import print_function
# python 2.x to make print assignment work

"""
    @author: Fabian Brix
    .py script to consolidate data from different sources (agmarknet, fca, shapefile administrational data)

"""

import glob
from os import path
#import sys
import pickle
import logging
import pandas as pd
import numpy as np
import json
import re

import address_cleaner

data_dir = '../../../data'
admin_dict = {}

# Remove Market Removal
market_corrections = {
   'Mkt.'  : ' Market ',
   'Market' : '',
   'Yard' : '',
   ',RBZ' : '',
   '(AP)' : '',
   '(UP)' : '',
   '(BH)' : '',
   '(KA)' : '',
   ' Hqs' : '',
   '(main)' : '',
   # google geocoding was able to make this inference itself
   'V.Kota' : 'Venkatagirikota',
   'Fish,Poultry & Egg , Gazipur' : 'Poultry Market, Gazipur',
   'Udaipur\(F&V\)' : 'Vegetable Market, Udaipur'
}

### dict for replacement of substrings of address cols --> write pandas code
# have to add 2nd bracket with state ['State']
# add pin codes: http://www.pincodein.com/City-Taluka-pin-code/Ahmedabad/11.aspx?StateId=11&District=Ahmedabad&Taluk=City+Taluka
# or use present pin codes to infer (taluk &) district

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
logger.addHandler(logging.FileHandler('test.log', 'a'))
print = logger.info

def extract_district(apmc, sec, addressCleaner):
    dist = addressCleaner.assign_district(apmc)
    if not dist:
        dist = addressCleaner.assign_district(sec)
    """
    move this code to cleaner
    if len(dist) > 1:
        print('#####')
        print('!district ambiguous!'.upper())
        print('#####')
    """
    return dist

def extract_taluk(apmc, sec, addressCleaner, taluks, market):
    taluk = addressCleaner.assign_taluk(apmc, taluks, market)
    if not taluk:
        taluk = addressCleaner.assign_taluk(apmc, taluks)
    """
    if len(taluk) > 1:
        print('#####')
        print('!taluk ambiguous!'.upper())
        print('#####')
    """
    return taluk

def loc_similarity(extr_loc, pin_loc):
    sim = 0
    if extr_loc[0]:
        sim = address_cleaner.ngram_similarity(extr_loc[0], pin_loc[0])
    if extr_loc[1]:
        sim = sim + address_cleaner.ngram_similarity(extr_loc[1], pin_loc[1])
    elif not extr_loc[1] and not pin_loc[1]:
        # add bonus for matching empty fields
        sim = sim + 1
    return sim

def to_nan(s):
    if s == None:
        s = np.nan
    return s

def extract_location(row, pos_df, addressCleaner, taluks):
    # assign extracted district and taluk values (lists in double quotes)
    apmc_address = row['APMC Address']
    sec_address = row['Secretary Address']
    market = row['Market']
    # get pin only from APMC address
    pin = row['Pin']
    dist = extract_district(apmc_address, sec_address, addressCleaner)
    if dist:
        taluks = list(filter(bool, list(pos_df.loc[pos_df['District'] == dist]['Taluk'].dropna().unique())))
    taluk = extract_taluk(apmc_address, sec_address, addressCleaner, taluks, market)
    loc = (dist, taluk)
    if pin:
        pin_df = pos_df.loc[pos_df['PIN'] == int(pin)]
        if not pin_df.empty:
            # apply function that yields most similar df
            locs = []
            for row in pin_df.itertuples():
                idx, p, t, d, s = row
                pin_loc = (d,t)
                sim = loc_similarity(loc, pin_loc)
                locs.append((pin_loc, sim))
            # sort and take first
            locs.sort(key=lambda x: x[1], reverse=True)
            sim_loc = locs[0][0]
            dist = sim_loc[0]
            # reassign taluk if not empty
            if sim_loc[1]:
                taluk = sim_loc[1]

    if taluk and not dist:
        dists = list(filter(bool, pos_df.loc[pos_df['Taluk'] == taluk]['District'].unique()))
        if len(dists) == 1:
            dist = dists[0]

    market_cleaned = market
    if dist and str(dist) != 'nan' and dist in market:
        # market.replace(dist, '')
        market_cleaned = re.sub(r'%s\W'%dist, '', market_cleaned).strip().strip(',').lstrip('(').rstrip('(').rstrip(')')
        if not market_cleaned:
            market_cleaned = dist 

    print((dist, taluk))
    return pd.Series([market_cleaned, to_nan(dist), to_nan(taluk)], ['Market_Cleaned', 'District', 'Taluk'])

# extracted from postcodes file
def load_admin_dict(fn):
    admin_dict = pickle.load(open(fn, 'rb'))
    return admin_dict

def main():
    pos_df = pd.DataFrame.from_csv(path.join(data_dir, 'admin/POs_data_cleaned.csv'))
    #admin_dict = load_admin_dict('admin_hierarchy_dict.p')
    pos_df.fillna('',inplace=True)
    #pos_df['taluk'] = pos_df['taluk'].str.replace(r'(Mandall|Mandalam|\(mdl\)|Mandal)', '', re.IGNORECASE).str.strip()
    #pos_df['district'] = pos_df['district'].str.strip()

    corrections_dict = json.loads(open('../cleaning/corrections.json').read())

    ### iterate over market files by state
    state_market_dir = path.join(data_dir, 'markets')
    market_dfs = []
    for state_market_file in [f for f in glob.glob(state_market_dir+'/*.csv') if not 'localized' in f]:
        state_market_df = pd.DataFrame.from_csv(state_market_file, index_col = None)
        if 'Orissa' in state_market_file:
            state_market_df['State'] = state_market_df['State'].str.replace('Orissa', 'Odisha')
        if 'Chattisgarh' in state_market_file:
            state_market_df['State'] = state_market_df['State'].str.replace('Chattisgarh', 'Chhattisgarh')
        if 'Uttrakhand' in state_market_file:
            state_market_df['State'] = state_market_df['State'].str.replace('Uttrakhand', 'Uttarakhand')

        # remove old version of Punchaipuliyampatti market
        state_market_df = state_market_df[(state_market_df.Market != 'Punchaipuliyampatti') | (state_market_df.Regulated == False)]
        state_market_df['Market'] = state_market_df['Market'].replace(market_corrections, regex=True)

        state_market_df['Market'] = state_market_df['Market'].str.strip()
        state = state_market_df['State'][0]
        #states = list(pos_df['State'].unique())
        districts = list(filter(bool, list(pos_df.loc[pos_df['State'] == state]['District'].dropna().unique())))
        ### filter removes empty strings
        taluks = list(filter(bool, list(pos_df.loc[pos_df['State'] == state]['Taluk'].dropna().unique())))
        if not state in corrections_dict:
            corrections_dict[state] = {}
        addressCleaner = address_cleaner.AddressCleaner(state, districts, corrections_dict[state])
        # extract pin
        state_market_df['Pin'] = state_market_df['APMC Address'].apply(address_cleaner.find_pin)
        state_market_df['APMC Address'] = state_market_df['APMC Address'].apply(addressCleaner.normalize)
        state_market_df['Secretary Address'] = state_market_df['Secretary Address'].apply(addressCleaner.normalize)
        # pass a cleaner object to apply

        address_df = state_market_df.apply(extract_location, axis=1, args=[pos_df, addressCleaner, taluks])
        state_market_df.fillna(value=np.nan, inplace=True)

        num_markets = len(address_df.index)
        num_ass_dists = address_df[['District']].count()
        num_ass_taluks = address_df[['Taluk']].count()
        print(num_ass_dists)
        print(num_ass_dists/num_markets)
        print(num_ass_taluks)
        print(num_ass_taluks/num_markets)
        print(num_markets)

        index = address_df.index[address_df['District'].apply(pd.isnull)]

        full_df = pd.concat([state_market_df, address_df], axis=1)
        # Problem: Grain)(Chandpole, Grain)(Mandor, Grain)(Phalodi
        full_df = full_df.replace(r'\s\)', ')', regex=True)
        full_df = full_df.replace(r'\(\)', '', regex=True)
        full_df = full_df.replace(r'\s+', ' ', regex=True)
        full_df = full_df.replace(r'\s+,\s*', ', ', regex=True)
        full_df = full_df.replace(r'\s*,$', '', regex=True)
        full_df['Market_Cleaned'] = full_df['Market_Cleaned'].str.strip()
        full_df['Market'] = full_df['Market'].str.strip()
        no_district = full_df.ix[index][['APMC Address', 'Secretary Address', 'District', 'Taluk', 'Pin']]
        ### need to delete balrampur market duplicate here already
        pd.DataFrame.to_csv(no_district, path.splitext(state_market_file)[0]+'_localized_unassigned.csv')
        print(no_district)
        market_dfs.append(full_df[['Market_Cleaned', 'Market', 'State', 'District', 'Taluk']])
        pd.DataFrame.to_csv(full_df, path.splitext(state_market_file)[0]+'_localized.csv')

    market_df = pd.concat(market_dfs)
    #market_df = market_df.drop_duplicates()
    market_df['Index'] = market_df[['State', 'Market_Cleaned']].apply(lambda x: '_'.join(x), axis=1)
    # remove Balrampur duplicate
    market_df = market_df.drop_duplicates(subset='Index', take_last=True)
    print(market_df['Index'].value_counts())
    market_df = market_df.set_index('Index')
    js = market_df.to_json(orient = 'index')
    with open(path.join(data_dir, 'admin/market_locations_serialized.json'), 'w') as f:
        f.write(js)
        f.close()
    #### OPTIONAL: merge dataframes
    return

if __name__ == "__main__":
    main()
