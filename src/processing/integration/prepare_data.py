import os
import sys
from os import path, listdir
import glob
import json
import pylev
import ngram
import time
import re
from datetime import datetime
import math
import pandas as pd
import numpy as np
import odo
import logging
import yaml
from bson import json_util

from mongo_handler import MongoDB

data_dir = '../../../data'

### TODO: use shelve to save such python dictionaries instead of json
market_corrections = {
   'Mkt.'  : '',
   'Market' : '',
   'Yard' : '',
   ',RBZ' : '',
   '(AP)' : '',
   '(UP)' : '',
   '(BH)' : '',
   '(KA)' : '',
   ' Hqs' : '',
   '(main)' : '',
   r'\s+' : ' ',
   r' ,$' : '',
   # google geocoding was able to make this inference itself
   'V.Kota' : 'Venkatagirikota',
   'Fish,Poultry & Egg , Gazipur' : 'Fish and Poultry Market, Gazipur',
   'Udaipur\(F&V\)' : 'Vegetable Market, Udaipur'
}

# http://stackoverflow.com/questions/455580/json-datetime-between-python-and-javascript/32224522#32224522
class DateTimeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
    	if isinstance(obj, datetime.date) or isinstance(obj, datetime.datetime):
    		return obj.isoformat()
    	else:
            return super(DateTimeJSONEncoder, self).default(obj)


def date_handler(obj):
	if hasattr(obj, 'isoformat'):
		return obj.isoformat()
	else:
		raise TypeError('object does not have method isoformat!')
		return None

def prepend_century(year):
	if 59 <= year <= 99:
		year = '19' + str(year)
	else:
		year = '20' + str(year)
	return year

def get_state_census():
	return

### TODO: differentiate between bulk and online insert
def prepare_fca():	
	"""
	fca_dir = path.join(data_dir, 'fca/csv')
	init_dir = os.getcwd()
	os.chdir(fca_dir)
	files = glob.glob('*.csv')
	for file in files:
		type decide if Retail or Whole
		df = pd.DataFrame.from_csv(file, index_col = None)	
		fca_dict = {
			centre: centre_name,
			location: coordinates,
			### put district id here later, through lookup at insertion time
			district: ..,
			date: date,
			'commodity': comm,
			'price': ..,
			'type' : type
		}
	"""
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
	#dfdict = {}
	### NOTE: quick fix, because no regex in blaze batch cleaning
	df = df.replace({ 'market' : market_corrections}, regex=True)
	### NOTE: if downloaded df empty, don't save bulletin --> implemented in get_agmarknet, still needs testing
	### TODO: FILL EMPTY HERE

	df['date'] = df['date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
	#df['weight'] = df['weight'].astype(float)
	#df['price'] = df['price'].astype(float)
	location_df = df.apply(add_location, axis=1, args=[market_locations])
	fdf = pd.concat([df, location_df], axis=1)
	#dfjson = fdf.to_json(orient='records')
	#dfdict = json.loads(dfjson)
	#return dfdict
	return fdf

### TODO: integrate with FCA data
def prepare_commodities(market_locations, mode, overwrite):
	start = time.time()
	agmarknet_dir = path.join(data_dir, 'agmarknet')
	if mode == 'batch':
		src_dir = path.join(agmarknet_dir, 'by_commodity')
	else:
		src_dir = path.join(agmarknet_dir, 'by_date_and_commodity')
	folders = listdir(src_dir)
	init_dir = os.getcwd()
	for folder in folders:
		if not path.isdir(path.join(src_dir, folder)):
			continue
		outdir = os.path.abspath(path.join(src_dir, folder, 'integrated'))
		if not path.isdir(outdir):
			os.makedirs(outdir)

		indir = os.path.abspath(path.join(src_dir, folder, 'cleaned'))
		os.chdir(indir)
		#os.chdir(data_dir)
		files = glob.glob('*cleaned.csv')
		for filename in files:
			os.chdir(indir)
			outfile = filename.replace('_cleaned.csv', '_localized.csv') # from commodiy name
			outpath = path.join(outdir, outfile)
			if os.path.isfile(outpath):
				if overwrite:
					os.remove(outpath)
				else:	
					print('Skipping {} ..'.format(outpath))
					continue
			print('Loading {} ..'.format(filename))
			header = 0 
			if mode == 'online':
				header = False
			df = pd.DataFrame.from_csv(filename, index_col=None, header=header)	
			#df.columns = ['date', 'state', 'market', 'commodity', 'variety', 'weight', 'min', 'max', 'modal']
			# NOTE: assigning header not necessary, since contained in cleaned csv
			print('Adding location ..')
			df = create_comm_doc(df, market_locations)
			df['date'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
			### TODO: further modify df
			df = df.rename(columns={'arrival': 'totalTonnage'}})
			df.insert(4, 'varietyTonnage', np.nan)
			os.chdir(init_dir)
			### TODO:
			# - batch: save to => by_commodity/integrated
			# - online: save to => by_state_and_commodity/integrated
			### TODO: write script that stacks integrated files to batch

			print('Saving {} ..'.format(filename))
			# NOTE: dumping integrated df to csv (instead of json) => use for analysis
			odo.odo(df, outpath)
			print()
			# df.to_csv(outpath, index=False)
	end = time.time()
	unit = 'minutes'
	elapsed = int((end - start) // 60)
	if elapsed > 60:
		elapsed = elapsed // 60
		unit = 'hours'
	print('Commoditiy integration ({2}) completed in {0} {1}!'.format(elapsed, unit, mode))
	return


def prepare_states(locs):
	census = json.load(open(path.join(data_dir, 'census', 'population_states.json'), 'r'))
	# {"States" : {"Statename", ... }, }
	census = census['States']
	#shapes = json.load(open(path.join(data_dir, 'integrated', 'admin', 'states.geojson'), 'r'))
	state_dicts = []
	#print(census.keys())
	### TODO: load shape dict
	census_india = json.load(open(path.join(data_dir, 'census', 'population_india_total.json'), 'r'))
	census_india = census_india['India']
	"""
	shape_india = json.load(open(path.join(data_dir, 'admin', 'geo', 'india_simp.geojson'), 'r'))
	shape_india = shape_india['features'][0]
	shape_india['properties'] = {
		'level' : 'Country',
		'name' : 'India',
		'name_local' : 'Bharat'
	}
	"""

	india_dict = {
		'_id' : 'India',
		'states' : list(locs.keys()),
		'census' : census_india['India'],
		#'shape' : shape_india,
		'commodities' : None
	}
	state_dicts.append(india_dict)

	census_states = census.keys() #list(map(lambda x: x.replace(r'&', 'and').title(), census.keys()))
	for state in locs.keys():
		state_cands = []
		for cstate in census_states:
			if pylev.levenshtein(state.lower(), cstate.lower()) <=2:
				state_cands.append(cstate)
		# statenames need to be the same is in population data --> create a yaml config somewhere with statenames to agree on
		# => for now assume the statenames are the same
		cstate = state_cands[0]
		census_dict = census[cstate]
		# remove key from dict so as to reduce # of future checks
		del census[cstate]
		districts = list(locs[state].keys())
		### USE '_id' here directly instead of NAME?
		state_dict = {
			### put postodes statename with 
			'_id' : state,
			'districts' : districts,
			'census' : census_dict,
			#'shape' : shapes[state],
			'commodities' : None
		}
		state_dicts.append(state_dict)
	statesjson = json.dumps(state_dicts)
	# move to integrated/admin
	# ready for bulk insert
	### TODO: handle exceptions?
	with open(path.join(data_dir, 'integrated', 'admin', 'states.json'), 'w') as f:
		f.write(statesjson)
		f.close()
	return

def get_dist_census(census, dist, state):
	census_dict = {}
	dist_cands = []
	for cdist, value in census.items():
		# higher threshold, additional ngram sorting
		# get only districts that have 'state' = field
		#print(state, value['state'])
		#excepts = ['Harmipur', 'Cachar', 'Pathankot', 'Aurangabad']
		if value['state'] != state:# or dist in excepts:
			continue
		if dist == cdist:
			dist_cands.append(cdist)
		elif dist in cdist:
			dist_cands.append(cdist)
		elif cdist in dist:
			dist_cands.append(cdist)
		elif pylev.levenshtein(dist, cdist) <= 3:
			dist_cands.append(cdist)
	if dist_cands:
		w_list = list(map(lambda x: (x, ngram.NGram.compare(dist.lower(), x.lower())), dist_cands))
		w_list.sort(key=lambda x: x[1], reverse=True)
		cdist = w_list[0][0]
		census_dict = census[cdist]
		del census[cdist]
	else:
		census_dict = None
	return census_dict

def get_boundary():
	return

def prepare_districts(locs):
	### TODO: load census location names match dict
	### TODO: load geo location names match dict
	census = json.load(open(path.join(data_dir, 'census', 'population_districts.json'), 'r'))
	census = census['Districts']
	#shapes = json.load(open(path.join(data_dir, 'integrated', 'admin', 'districts.geojson'), 'r'))
	dist_dicts = []
	for state in locs.keys():
		dists = list(locs[state].keys())
		for dist in dists:
			census_dict = get_dist_census(census, dist, state)
			### ADD taluks at later stage if cleaning works out properly?
			### to be seen: "None maps intuitively to MongoDB types when using pymongo"
			### NOTE: do not insert shapes, json is directly sent to client to be displayed, 
			# db contains data to be fetched on request
			### TODO: WHAT IF WE WANT TO ADD SHAPES TO SUPPORT GEOSPATIAL QUERIES:
			### What use cases would require geospatial queries
			"""
			shape = None
			if dist in shapes:
				shape = shapes[dist]
			else:
				print(dist)
			"""	
			dist_dict = {
				### put postcodes district name with census_dict
				'name' : dist,
				### dist names not unique: let _id be autogenerated
				'state_id' : state,
				'markets' : None,
				'census' : census_dict,
				'commodities' : None
			}
			dist_dicts.append(dist_dict)

	districtsjson = json.dumps(dist_dicts)
	### TODO: handle exceptions?
	with open(path.join(data_dir, 'integrated', 'admin', 'districts.json'), 'w') as f:
		f.write(districtsjson)
		f.close()
	return

def match_topo_state_names(locs, db):
	#geojson = json.load(open(path.join(data_dir, 'admin', 'geo', 'states_simp.geojson'), 'r'))
	topojson = json.load(open(path.join(data_dir, 'admin', 'geo', 'states_simp.json'), 'r'))
	#geo_prop_list = list(map(lambda x: (x['properties']['NAME_1'], x['properties']['VARNAME_1']), geojson['features']))
	states = list(locs.keys())
	### NOTE: state_names are identical
	### Match dict can be globally expanded and reused
	#state_shape_dict = {}
	state_match_dict = {}
	state_match_dict['Delhi'] = 'NCT of Delhi'
	#for shape in geojson['features']:
	shapes = []
	for shape in topojson['objects']['states_simp']['geometries']:
		state_cands = []
		name = shape['properties']['NAME_1']
		print(shape['properties'])
		varname = shape['properties']['VARNAME_1']
		if varname:
			varname = varname.replace('&', 'and')
			alt_names = varname.split('|')
		else:
			alt_names = []

		for state in states:
			if pylev.levenshtein(name.lower(), state.lower()) <= 3:
				state_cands.append(state)
		if state_cands:
			mlist = list(map(lambda x: (x, ngram.NGram.compare(x.lower(), name.lower())), state_cands))
			print(mlist)
			mlist.sort(key=lambda x: x[1], reverse=True)
			mstate = mlist[0][0]
			states.remove(mstate)
			state_match_dict[name] = mstate
		else:
			for alt_name in alt_names:
				for state in states:
					if pylev.levenshtein(alt_name.lower(), state.lower()) <= 3:
						state_cands.append(state)
			if state_cands:
				mlist = list(map(lambda x: (x, ngram.NGram.compare(x.lower(), name.lower())), state_cands))
				print(mlist)
				mlist.sort(key=lambda x: x[1], reverse=True)
				mstate = mlist[0][0]
				states.remove(mstate)
				state_match_dict[name] = mstate
			else:
				print('No match found for {}'.format(name))
		shape['properties'] = {
			'level' : 'State',
			'name' : state_match_dict[name],
		}
		shape['id'] = state_match_dict[name]
		shapes.append(shape)
		#state_shape_dict[state_match_dict[name]] = shape
	topojson['objects']['states_simp']['geometries'] = shapes
	return (state_match_dict, topojson)
	#return (state_match_dict, state_shape_dict)

def match_topo_dist_names(locs, state_match_dict, db):
	### given uniqueness:
	state_match_dict = dict(zip(state_match_dict.values(), state_match_dict.keys()))
	#geojson = json.load(open(path.join(data_dir, 'admin', 'geo', 'districts_simp.geojson'), 'r'))
	topojson = json.load(open(path.join(data_dir, 'admin', 'geo', 'districts_simp.json'), 'r'))
	#features = list(geojson['features'])
	features = topojson['objects']['districts_simp']['geometries']
	### GEOJSON DIFFERENTLY
	### TODO: assign Junagadh to another Gujarat
	#geo_prop_list = list(map(lambda x: x['properties'], geojson['features']))
	dist_match_dict = {}
	dist_shape_dict = {}
	mod_shapes = []
	for state in list(locs.keys()):
		### get only state specific properties (state correction lookup) 
		print(state)
		print(state_match_dict[state])
		#geo_list_by_state = list(map(lambda x: x["properties"], features))
		#names = list(filter(lambda x: x["NAME_1"] == state_match_dict[state], geo_list_by_state))
		#shapes = zip(shapes, names)
		### managed to circumvent NAME_1 key error?
		shapes = [shape for shape in features if 'NAME_1' in shape['properties']]
		shapes = [shape for shape in shapes if shape['properties']['NAME_1'] == state_match_dict[state]]
		#shapes = list(filter(lambda x: x["properties"]["NAME_1"] == state_match_dict[state], features))
		#print(names)
		dists = list(locs[state].keys())
		print(len(dists))
		for shape in shapes:
			print(shape)
			prop = shape['properties']
			print(prop)
			name = prop['NAME_2']
			if name == 'Junagadh':
				continue
			if name == 'Ladakh (Leh)':
				name = 'Leh'
			if name == 'Andaman Islands':
				dist_match_dict[name] = ['North And Middle Andaman', 'South Andaman']
				shape['properties'] = {
					'level' : 'District',
					'name' : 'North and Middle Andaman',
					'state' : state
				}
				dist_shape_dict['North And Middle Andaman'] = shape
				shape['properties'] = {
					'level' : 'District',
					'name' : 'South Andaman',
					'state' : state
				}
				dist_shape_dict['South Andaman'] = shape
				continue

			name = re.sub('\(.*\)', '', name)
			name = re.sub('(\w+) Imphal', 'Imphal \\1', name)
			name = name.replace('Pashchim', 'West')
			name = name.replace('Dakshin', 'South')
			name = re.sub('Uttar ', 'North ', name)
			name = name.replace('Purba', 'East')
			name = name.replace('South Kannad', 'Daskhina Kannada')
			name = name.replace('North Kannand', 'Uttar Kannada')
			name = name.replace('Greater Bombay', 'Mumbai')
			name = name.replace('Kavaratti', 'Lakshadweep')
			name = name.replace('Thoothukudi', 'Tutocorin')
			name = name.replace('Nawan Shehar', 'Nawanshahr')
			name = name.replace('Bagdam', 'Bugdam')
			name = name.replace('Kawardha', 'Kabirdham')
			if state == 'West Bengal':
				name = name.replace('Hugli', 'Hooghly')
				name = name.replace('Kochbihar', 'Cooch Behar')
				name = name.replace('Haora', 'Howrah')
			varname = prop['VARNAME_2']
			if varname:
				varname = varname.replace('&', 'and')
				alt_names = varname.split('|')
			else:
				alt_names = []
			dist_cands = []
			for dist in dists:
				if dist == name:
					dist_cands.append(dist)
				elif dist in name:
					dist_cands.append(dist)
				elif name in dist:
					dist_cands.append(dist)
				elif pylev.levenshtein(name.lower(), dist.lower()) <= 2:
					dist_cands.append(dist)
			if dist_cands:
				mlist = list(map(lambda x: (x, ngram.NGram.compare(x.lower(), name.lower())), dist_cands))
				mlist.sort(key=lambda x: x[1], reverse=True)
				mdist = mlist[0][0]
				dists.remove(mdist)
				dist_match_dict[name] = mdist
			else:
				for alt_name in alt_names:
					for dist in dists:
						if pylev.levenshtein(alt_name.lower(), dist.lower()) <= 5:
							dist_cands.append(dist)
				if dist_cands: 
					mlist = list(map(lambda x: (x, ngram.NGram.compare(x.lower(), name.lower())), dist_cands))
					mlist.sort(key=lambda x: x[1], reverse=True)
					mdist = mlist[0][0]
					print(mdist)
					dists.remove(mdist)
					dist_match_dict[name] = mdist
				else:
					print(name)
					print('No match found!')
			# andaman and nicobar special case
			shape['properties'] = {
				'level' : 'District',
				'name' : dist_match_dict[name],
				'state' : state
			}

			res = db.fetch('districts', { 'state_id' : state, 'name' : dist_match_dict[name] }, False)
			obj_id = res[0]['_id']
			print(obj_id)
			shape['id'] = obj_id
			mod_shapes.append(shape)
			#dist_shape_dict[dist_match_dict[name]] = shape
	topojson['objects']['districts_simp']['geometries'] = mod_shapes
	return (dist_match_dict, topojson)
	#return (dist_match_dict, dist_shape_dict)

def prepare_topology(locs, db):
	### TODO: need to add id properties to states + districts
	## put 'id' field under 'geometries' alongside 'properties' dict
	### => prepare_state_topology()
	### => prepare_district_topology()
	state_match_dict, state_topology = match_topo_state_names(locs, db)	
	dist_match_dict, dist_topology = match_topo_dist_names(locs, state_match_dict, db)	

	json.dump(state_topology, open(path.join(data_dir, 'integrated', 'admin', 'states.json'), 'w'))
	with open(path.join(data_dir, 'integrated', 'admin', 'districts.json'), 'w') as f:
		f.write(json_util.dumps(dist_topology))
		f.close()

	prepare_market_features(db)
	### TODO: fetch('districts|markets', {}, False)
	#state_match_dict, state_shape_dict = match_topo_state_names(locs)	
	#dist_match_dict, dist_shape_dict = match_topo_dist_names(locs, state_match_dict)	
	#json.dump(state_shape_dict, open(path.join(data_dir, 'integrated', 'admin', 'states.geojson'), 'w'))
	#json.dump(dist_shape_dict, open(path.join(data_dir, 'integrated', 'admin', 'districts.geojson'), 'w'))
	### Use state match dict to find district match
	return 

def create_market_doc(row, logger, state_coords_df):
	established = row['Established (Year)']
	## problem with 1/2/3 digit dates
	if isinstance(established, float) or int(established) == 0:
		established = None
	else:
		if len(str(established)) == 1:
			established = '200' + str(established)
		if len(str(established)) == 2:
			established = prepend_century(int(established))
		if len(str(established)) == 3:
			established = str(established) + '0'
		established = datetime.datetime.strptime(str(established), '%Y').date()

	pin = row['Pin']
	if not math.isnan(pin):
		pin = int(pin)
	else:
		pin = None

	print(row)
	row = row.str.replace(np.nan, None)
	print(row)

	#coordinates, geojson = geocoding.get_market_coordinates(pin, row['Market_Cleaned'], row['District'], row['State'])
	coords_row = state_coords_df.loc[state_coords_df['Market'] == row['Market']]
	coordinates = {
		'lat' : coords_row['Lat'],
		'lng' : coords_row['Lng']
	}
	### TODO: put market cleaned
	market_dict = {
		'market' : row['Market'],
		'state': row['State'],
		'district' : row['District'],
		'taluk': row['Taluk'],
		'regulated': row['Regulated'],
		'established': established,
		'num_commodities_regulated': row['# Regulated Commodities'],
		'cleaning_or_grading': row['Cleaning/Grading'],
		'num_coldstorage': row['# Cold Storage Facilities'],
		'address' : { 
			'pin' : pin,
			'apmc_address' : row['APMC Address'],
			'apmc_secretary_address': row['Secretary Address']
		},
		'transport' : {
			'incoming' : row['Incoming Transport'],
			'outgoing' : row['Outgoging Transport'],
			'railway_dist' : row['Railway Distance (km)']
		},
		'business' : {
			'market_fee' : row['Market Fee'],
			'commission' : row['Commission'],
			'sale_process' : row['Sale Process'],
			'payment_process' : row['Payment Process']
		},
		'accounting' : {
			'market_income' : row['Market Income'],
			'market_expenditute' : row['Market Expenditure'],
			'profit' : row['Profit'],
			'apmc_reserves' : row['APMC Reserves'],
			'apmc_liabilities' : row['APMC liabilities']
		},
		'coordinates' : coordinates
	}
	return market_dict

### FIRST, INSERT states, districts ==> object ids in markets
def prepare_markets(cont = False):
	logger = logging.getLogger('markets_prepare')
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler('prepare_markets.log')
	fh.setLevel(logging.INFO)
	logger.addHandler(fh)
	curr_dir = os.getcwd()
	src_dir = path.join(data_dir, 'markets')
	os.chdir(src_dir)
	coordinates_df = pd.DataFrame.from_csv(path.join(data_dir, 'admin/geo', 'market_coordinates.csv'))
	files = glob.glob('*localized.csv')
	### TODO: run geolocation separately?
	#done = ['Karnataka', 'Jharkand', 'Jammu and Kashmir', 'Himachal Pradesh', 'Haryana', 'Gujarat', 'Goa', 'Chhattisgarh', 'Bihar', 'Assam', 'Arunachal Pradesh', 'Andhra Pradesh']
	# TODO: WHEN NOT ALL MARKETS IN MARKET LOCATIONS?
	for file in files:
		### NOTE: header present in csv files
		df = pd.DataFrame.from_csv(file, index_col = None)	
		df = df.replace('*', np.nan)
		# the way I'm building the dict now I'll have to run an apply function that returns the dictionaries
		market_dict_list = []
		outfile = file.replace('_localized.csv', '.json').replace(' ', '-')
		outpath = path.join('..', 'integrated', 'markets', outfile)
		### HAVE TO DELETE JSON DUMPS BEFORE RERUNNING
		if os.path.isfile(outpath) and cont:
			continue
		state_coords_df = coordinates_df.loc[coordinates_df['State'] == list(df['State'].unique())[0]]
		print(outfile)
		print()
		for index, row in df.iterrows():
			logger.info('Processing market {}...'.format(row['Market_Cleaned']))
			logger.info('---')
			market_dict = create_market_doc(row, logger, state_coords_df)
			market_dict_list.append(market_dict)
			### connection aborted after several requests
			logger.info('...')
		#market_dict_list = df.apply(create_market_doc, axis=1).tolist()
		marketjson = json.dumps(market_dict_list, cls=DateTimeJSONEncoder)#default = date_handler)
		with open(outpath, 'w') as f:
			f.write(marketjson)
			f.close()
	### use market profiles that have gone through address assignment 
	os.chdir(curr_dir)
	return

def prepare_market_features(db):
	markets = db.fetch('markets', {}, False)
	feature_collection = {}
	market_features = []
	for market in markets:
		coords = market['coordinates']
		if isinstance(coords, list):
			coords = coords[0]
		market_feature = {
			"type": "Feature",
				"geometry": {
				"type": "Point",
				"coordinates": [coords['lng'], coords['lat']]
			},
			"id" : market['_id'],
			"properties": {
				"name": market['market'],
				'state': market['state'],
				'district' : 'null' if isinstance(market['district'], float) else market['district'],
				'taluk': 'null' if isinstance(market['taluk'], float) else market['taluk'],
				'regulated': market['regulated'],
				'established': 'null' if isinstance(market['established'], float)  else market['established']
		  	}	
		}
		print(type(market['taluk']))
		print(market['taluk'])
		print(market_feature)
		market_features.append(market_feature)
	feature_collection['type'] = 'FeatureCollection'
	feature_collection['features'] = market_features
	featurejs = json_util.dumps(feature_collection)
	with open(path.join(data_dir, 'integrated', 'admin', 'markets.geojson'), 'w') as f:
		f.write(featurejs)

def create_prod_match():
	return

def match_crop(prod_crop_names):
	correction_dict = {}
	return correction_dict

def prepare_production(db):
	### TODO: again need to integrate districts	
	##df = pd.DataFrame.from_csv(path.join(data_dir, folder, file), index_col = None, header=None)	
	### TODO: no grouping here, just json, state integration, district integration, crop integration
	#prod_crops = list(df['Crop'].unique())
	### TODO: WRITE DAT CODE
	### state > district > crop > year > total (kharif, rabi, summer, winter, etc.)	
	### TODO: make sure crop names match with crop names 
	return

def main(call, overwrite=False, mode=False):
	if overwrite:
		overwrite = True
		print(overwrite)
	config = yaml.load(open('../../db/db_config.yaml', 'r'))
	db = MongoDB(config['meteordb'], config['address'], config['meteorport'])

	locs = json.load(open(path.join(data_dir, 'admin', 'locations_serialized.json'), 'r'))
	# schema: {"Statename" : {"DistrictName1" : ["Taluks"], "DistrictName2": ["Taluks"], .. }}
	market_locs = json.load(open(path.join(data_dir, 'admin', 'market_locations_serialized.json'), 'r'))
	if call == 'topology':
		prepare_topology(locs, db)
	elif call == 'states':
		prepare_states(locs)
	elif call == 'districts':
		prepare_districts(locs)
	elif call == 'markets':
		prepare_markets(cont = True)
	elif call == 'commodities':
		if not mode in ['batch', 'online']:
			print('Error! Processing mode needs to be set')
			sys.exit()
		prepare_commodities(market_locs, mode, overwrite)

if __name__ == '__main__':
	check = ['commodities', 'states', 'districts', 'markets', 'topology']
	if len(sys.argv) > 1 and sys.argv[1] in check:
		main(*sys.argv[1:])
	else:
		print('usage: {0} (states|districts|markets|commodities|topology) overwrite [batch|online] '.format(sys.argv[0]))