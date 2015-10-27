import glob
import sys
import os
from os import path, listdir
import pandas as pd
import yaml, json
import re
import numpy as np
import logging
from datetime import datetime
import math

# parsing isoformatted dates from json dumps
# d = dateutil.parser.parse(s)
import dateutil.parser

"""
	figuring out imports:
	http://python-notes.curiousefficiency.org/en/latest/python_concepts/import_traps.html
"""

"""
	- ways of printing: print(q, p, p * q, sep=",")
	- string format example: "Art: {a:5d},  Price: {p:8.2f}".format(a=453, p=59.058)
	- this would be a good class to try out unit testing

"""

category_mappings = {
	'Live' : 'Livestock'
}

data_dir = '../../../data'

from mongo_handler import MongoDB

def create_state_dict():
	# need population data
	# need production data --> separate collection (collection for district --> aggregation: collection for state)
	# need land-use --> separate collection
	# need fertilizer

	# from where to load data?
	return

def assemble_districts():
	return

def to_nan(s):
	if s == None:
	    s = np.nan
	return s

def add_location(row, market_locations):
	# Problem: market names from commodity files and markets are not integrated
	### NEED A SYSTEM THAT DOES PERIODICAL CLEANING ACCORDING TO CERTAIN MAPPINGS
	# what to do if market is given in series but does not have a profile
	index = '_'.join([row['state'].strip(), row['market'].strip()])
	dist = None
	taluk = None
	if not index in market_locations:
		index = re.sub('(\(.*\))', '', index)
	if 'Dimapur' in index:
		index = 'Nagaland_Dimapur'
	if index in market_locations:
		dist = market_locations[index]['District']
		taluk = market_locations[index]['Market']

	# TODO: WHEN MARKET NOT IN MARKET LOCATIONS?
	# check if market can be found in market collection. 
	# --> If not, insert basic version with only market and state name fields filled

	# else add nan values - except for state (district aggregation still possible)
	# states names of final df have to again be cleaned
	return pd.Series([to_nan(dist), to_nan(taluk)], ['district', 'taluk'])

def create_comm_doc(df, market_locations):
	dfdict = {}
	if not df.empty:
		### TODO: FILL EMPTY HERE
		df.columns = ['date', 'state', 'market', 'commodity', 'variety', 'weight', 'min', 'max', 'modal']
		df.drop('min', axis=1, inplace=True)
		df.drop('max', axis=1, inplace=True)
		# NOTE: assumption of normal distribution since underlying price discovery process not known: 
		# -> modal price = mean price
		df.rename(columns={'modal': 'price'}, inplace=True)
		df = df.apply(lambda x: x.astype(str).str.strip(), axis=1)
		df['state'] = df['state'].str.replace('Orissa', 'Odisha')
		df['state'] = df['state'].str.replace('Chattisgarh', 'Chhattisgarh')
		df['state'] = df['state'].str.replace('Uttrakhand', 'Uttarakhand')
		### is it really dmY? -> yes
		df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").date())
		df = df.replace(r'-$', np.nan, regex=True)
		df = df.fillna(method='ffill')
		df = df.replace(r'\*$', np.nan, regex=True)
		print(df.head())
		df['weight'] = df['weight'].astype(float)
		df['price'] = df['price'].astype(float)
		location_df = df.apply(add_location, axis=1, args=[market_locations])
		fdf = pd.concat([df, location_df], axis=1)
		dfjson = fdf.to_json(orient='records')
		dfdict = json.loads(dfjson)
	return dfdict

### TODO: fill dates in commodity collections
def fill_dates():
	return	

### TODO: use POs dict (uncleaned taluk) as basis 
def insert_states(db):
	docs = json.load(open(path.join(data_dir, 'integrated', 'admin', 'states.json'), 'r'))
	### insert documents in bulk
	db.insert('states', docs)
	return

def insert_districts(db):
	docs = json.load(open(path.join(data_dir, 'integrated', 'admin', 'districts.json'), 'r'))
	db.insert('districts', docs)
	# read .../data/integrated/admin/districts.json
	return

### FIRST, INSERT states, districts ==> object ids in markets
def insert_markets(db, logger):
	curr_dir = os.getcwd()
	src_dir = path.join(data_dir, 'integrated', 'markets')
	os.chdir(src_dir)
	files = glob.glob('*json')
	for file in files:
		print(file)
		docs = json.load(open(file, 'r'))
		### insert documents in bulk
		db.insert('markets', docs)
		### TODO: replace district field by objid or use district and state for dereferencing?
	os.chdir(curr_dir)
	return

### NOTE: IF header=None flag not set when loading dataframe, then first line is read as header!!! --> lose records
def insert_commodities(db, logger):
	### using commodity documents as well as serialized market locations from address assignment
	src_dir = path.join(data_dir, 'agmarknet/csv')
	folders = listdir(src_dir)
	market_locations = json.load(open(path.join(data_dir, 'admin/market_locations_serialized.json'), 'r'))
	### Dicts for handling of 'Other' category and automatic collection dropping for data renewal
	category_commodity_dict = {}
	commodity_category_dict = {}
	folders.remove('Other')
	for folder in folders:
		category = folder
		category_commodity_dict[category] = []
		files = listdir(path.join(src_dir, folder))
		prev_commodity = None
		for file in files:
			commodity = file.split('_')[0]
			if not commodity in commodity_category_dict:
				commodity_category_dict[commodity] = category
			if not commodity in category_commodity_dict[category]:
				category_commodity_dict[category] = category_commodity_dict[category] + [commodity]
			df = pd.DataFrame.from_csv(path.join(src_dir, folder, file), index_col = None, header=None)	
			if prev_commodity != commodity:
				logger.info(commodity)
			prev_commodity = commodity
			# avoiding empty bson objects
			dfdict = create_comm_doc(df, market_locations)
			if dfdict:
				db.insert(folder, dfdict)

	### Handle 'Other' category
	files = listdir(path.join(src_dir, 'Other'))
	category = 'Other'
	prev_commodity = None
	for file in files:
		commodity = file.split('_')[0]
		if prev_commodity != commodity:
			logger.info(commodity)
		if not commodity in commodity_category_dict:
			category = commodity_category_dict[commodity]
		else:
			commodity_category_dict[commodity] = category
			category_commodity_dict[category] = category_commodity_dict[category] + [commodity]
		df = pd.DataFrame.from_csv(path.join(src_dir, folder, file), index_col = None)	
		dfdict = create_comm_doc()
		if dfdict:
			db.insert(folder, dfdict)

	with open('comm_by_category.json', 'w') as jsfile:
		json.dump(category_commodity_dict, jsfile)
	with open('category_by_comm.json', 'w') as jsfile:
		json.dump(commodity_category_dict, jsfile)

def main(type):
    config = yaml.load(open('../../db/db_config.yaml', 'r'))
    logger = logging.getLogger('db_insert')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('insert.log')
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    db = MongoDB(config['meteordb'], config['address'], config['meteorport'])

    if type == 'commodities':
    	insert_commodities(db, logger)
    elif type == 'states':
    	insert_states(db)
    elif type == 'districts':
    	insert_districts(db)
    elif type == 'markets':
    	insert_markets(db, logger)
    return

if __name__ == "__main__":
	check = ['commodities', 'states', 'districts', 'markets']
	if len(sys.argv) > 1 and sys.argv[1] in check:
		main(*sys.argv[1:])
	else:
		print('usage: {} (states|districts|markets|commodities)'.format(sys.argv[0]))
