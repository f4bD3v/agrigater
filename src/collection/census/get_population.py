"""
import requests
import lxml.html

census_form_url = 'http://www.censusindia.gov.in/pca/default.aspx'
r = requests.get(census_form_url)
source = r.text
 -> resorted to manual download
"""

import glob
from os import path
import numpy as np
import pandas as pd
import json

data_dir = '../../../data/census/'

### TODO: add handle for overriding of census.csv
cleaning_dict = {
	### SHOULD MAKE THIS DIST NESTED BY STATE
	# Medinipur ==> Midnapore
	r'Medinipur' : 'Midnapore',
	r'Purba ' : 'East ',
	r'Purbi ' : 'East ',
	r'Paschim ' : 'West ',
	r'Pashchim ' : 'West ',
	r'Pashchimi ' : 'West ',
	r'Uttar ' : 'North ',
	r'^Dakshin ': 'South ',
	'Sahibzada Ajit Singh Nagar' : 'Mohali',
	'Y.S.R.' : 'Y.S.R. Cuddapah',
	'Subarnapur' : 'Sonapur',
	'South Twenty Four Parganas' : 'South 24 Parganas',
	'North Twenty Four Parganas' : 'North 24 Parganas',
	'Hugli' : 'Hooghly',
	r'^Garhwal$' : 'Pauri Garhwal',
	'Mahamaya Nagar' : 'Hathras',
	'Thoothukkudi' : 'Tuticorin',
	# fix north pradesh
	'North Pradesh' : 'Uttar Pradesh'
}

census_fn = path.join(data_dir, 'census.csv')
census_df = None
if not path.isfile(census_fn):
	concat = False
	for fn in glob.glob(path.join(data_dir, '*.CSV')):
		# clean single quotes from state and district fields
		state_frame = pd.DataFrame.from_csv(fn, parse_dates = False, index_col = False)
		state_frame.drop('State', axis=1, inplace=True)
		state_frame.drop('District', axis=1, inplace=True)

		state_frame = state_frame.replace(r'&', 'and', regex=True)
		state_frame = state_frame.replace(r'PUDUCHERRY$', 'Pondicherry', regex=True)
		state_frame['Name'] = state_frame['Name'].str.title()
		state_frame['Name'] = state_frame['Name'].str.replace(r'And ', 'and ')
		state_frame['Name'] = state_frame['Name'].str.replace(r'Of ', 'of ')
		state_frame['Name'] = state_frame['Name'].str.replace(r'Nct ', 'NCT ')
		#state_frame['State'] = state_frame['State'].str.replace('\d+', '\1')
		#state_frame['District'] = state_frame['District'].str.replace('\d+', '\1')
		state_frame['State'] = np.repeat(state_frame.at[0, 'Name'], len(state_frame))

		if concat:
			census_df = pd.concat([census_df, state_frame], axis=0)
		else:
			census_df = state_frame
			concat=True
		### TODO: clean state names

	census_df = census_df.replace(r'&', 'and', regex=True)
	census_df['Name'] = census_df['Name'].replace(cleaning_dict, regex=True)
	census_df['Name'] = census_df['Name'].str.replace(r'Rangareddy', 'K.V.Rangareddy')
	census_df['Name'] = census_df['Name'].str.replace(r'\s+', ' ')
	census_df['Name'] = census_df['Name'].str.replace(r'Bangalore$', 'Bangalore Urban')
	census_df['Name'] = census_df['Name'].str.replace(r'\n', '').str.strip()

	#census_df = census_df.replace(r'NCT OF ', '', regex=True)
	census_df.to_csv(path.join(data_dir, 'census.csv'))
else:
	census_df = pd.DataFrame.from_csv(census_fn, index_col=None)
# remove unwanted columns from census_df?
"""
	how to organize tabular data in json:
	http://json-stat.org/format/
"""

### instead of row iteration try to apply function: but then how to get index?
def serialize(census_df, level):
	df = census_df.loc[census_df['Level'] == level]
	df = df.drop('Level', axis=1)
	# split by name
	gb=df.groupby('Name')
	level_groups = [gb.get_group(x) for x in gb.groups]
	census_dict = {}	
	census_dict['level'] = level.title()
	if level != 'India':
		level = level+'s'
	groups = []
	for group in level_groups:	
		group_dict = {}
		type_dict = {}
		for index, row in group.iterrows():
			pop_dict = {
				'total': row['Total Population Person'], 
				'work': row['Total Worker Population Person'],
				'cultivator_main': row['Main Cultivator Population Person'],
				'cultivator_marginal': row['Marginal Cultivator Population Person'],
				'agrilabourers_main': row['Main Agricultural Labourers Population Person'],
				'agrilabourers_marginal': row['Marginal Agriculture Labourers Population Person'],
				'no_work': row['Non Working Population Person']
			}
			properties = { 'area': row['Area'], 'population': pop_dict }
			type_dict[row['TRU'].lower()] = properties
			if level == 'DISTRICTs':
				type_dict['state'] = row['State']

		### TODO: at later stage: add cleaned districts for state
		#if level == 'STATE':
		#	type_dict['districts'] = []

		### Seems to be order be preserving:
		name = group.loc[:, 'Name'].unique()[0]
		group_dict[name] = type_dict
		groups.append(group_dict)

	result = {}
	for g in groups:
		result.update(g)
	census_dict[level.title()] = result
	return census_dict

india_dict = serialize(census_df, 'India')
with open(path.join(data_dir, 'population_india_total.json'), 'w') as fp:
    json.dump(india_dict, fp)
state_dict = serialize(census_df, 'STATE')
with open(path.join(data_dir, 'population_states.json'), 'w') as fp:
    json.dump(state_dict, fp)
district_dict = serialize(census_df, 'DISTRICT')
with open(path.join(data_dir, 'population_districts.json'), 'w') as fp:
    json.dump(district_dict, fp)