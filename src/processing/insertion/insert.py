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
import odo
import time

# parsing isoformatted dates from json dumps
#import dateutil.parser
# d = dateutil.parser.parse(s)

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


"""
def insert_commodities(db, config):
	src_dir = path.join(data_dir, 'agmarknet', 'by_date_and_commodity')
	market_locations = json.load(open(path.join(data_dir, 'admin/market_locations_serialized.json'), 'r'))
	folders = listdir(src_dir)
	init_dir = os.getcwd()
	for folder in folders:
		print(folder)
		if not path.isdir(folder):
			continue
		os.chdir(path.join(src_dir, folder, 'cleaned'))
		files = glob.glob('*cleaned.csv')
		for filename in files:
			print('Loading {} ..'.format(filename))
			df = pd.DataFrame.from_csv(filename, index_col = None, header=False)	
			### TODO: assign header
			#df.columns = ['date', 'state', 'market', 'commodity', 'variety', 'weight', 'min', 'max', 'modal']
			print('Adding location ..')
			df = create_comm_doc(df, market_locations)
			df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
			collection = filename.replace('_cleaned.csv', '') # from commodiy name
			mongo_str = 'mongodb://{0}:{1}/{2}::{3}'.format(config['address'], config['meteorport'], config['meteordb'], collection)
			print('Inserting {} ..'.format(filename))
			# dump dataframe to json as in prepare_data
			odo.odo(df, mongo_str)

		os.chdir(init_dir)
	return
"""

"""
### NOTE: IF header=None flag not set when loading dataframe, then first line is read as header!!! --> lose records
def insert_commodities_batch(db, config, logger):
	start = time.time()
	### using commodity documents as well as serialized market locations from address assignment
	src_dir = path.join(data_dir, 'agmarknet', 'by_commodity')
	market_locations = json.load(open(path.join(data_dir, 'admin/market_locations_serialized.json'), 'r'))
	folders = listdir(src_dir)
	print(folders)
	init_dir = os.getcwd()
	for folder in folders:
		print(folder)
		os.chdir(path.join(src_dir, folder, 'cleaned'))
		batch_files = glob.glob('*cleaned.csv')
		print(batch_files)
		for filename in batch_files:
			print('Loading {} ..'.format(filename))
			#d = Data(filename)
			#print(bz.compute(d.head()))
			### TODO: HOW TO ADD MARKET LOCATIONS USING BLAZE???	
			df = pd.DataFrame.from_csv(filename, index_col=None)	
			print('Adding location ..')
			df = create_comm_doc(df, market_locations)
			df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
			collection = filename.replace('_stacked_cleaned.csv', '') # from commodiy name
			mongo_str = 'mongodb://{0}:{1}/{2}::{3}'.format(config['address'], config['meteorport'], config['meteordb'], collection)
			print('Inserting {} ..'.format(filename))
			odo.odo(df, mongo_str)	
		os.chdir(init_dir)
	end = time.time()
	unit = 'minutes'
	elapsed = int((end - start) // 60)
	if elapsed > 60:
		elapsed = elapsed // 60
		unit = 'hours'
	print('Batch insertion completed in {0} {1}!'.format(elapsed, unit))
	return

	"" "
	### Dicts for handling of 'Other' category and automatic collection dropping for data renewal
	# category_dict = json.load(open(path.join(data_dir, 'admin/category_dict.json'), 'r'))
	commodity_category_dict = {}
	folders.remove('Other')
	for folder in folders:
		category = folder
		files = listdir(path.join(src_dir, folder))
		prev_commodity = None
		for file in files:
			commodity = file.split('_')[0]
			df = pd.DataFrame.from_csv(path.join(src_dir, folder, file), index_col = None, header=None)	
			if prev_commodity != commodity:
				logger.info(commodity)
			prev_commodity = commodity
			dfdict = create_comm_doc(df, market_locations)
			# avoiding empty bson objects
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
	"""

### TODO: by creating an intermediate integrated/ store I have created myself a problem
# => still differentiate between stacked and online
def insert_commodities(df):
	df.drop('min', axis=1, inplace=True)
	df.drop('max', axis=1, inplace=True)
	# NOTE: assumption of normal distribution since underlying price discovery process not known: 
	# -> modal price = mean price
	df.rename(columns={'modal': 'price'}, inplace=True)
	return

### TODO: fill dates in commodity collections --> in manipulation/
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

def main(type):
    config = yaml.load(open('../../db/db_config.yaml', 'r'))
    logger = logging.getLogger('db_insert')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('insert.log')
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    db = MongoDB(config['meteordb'], config['address'], config['meteorport'])

    if type == 'commodities':
    	insert_commodities_batch(db, config, logger)
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
