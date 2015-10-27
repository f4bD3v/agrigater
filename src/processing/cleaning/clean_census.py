import json
from os import path
import collections

data_dir = '../../../data'

### PROBLEM: lots of NaNs

# Merge problem: two lower level dicts, need to add first field of 2nd level dict and all fields of 3rd level dict 
# urban: {area, dict}
# rural: {area, dict}
# total: {area, dict}
def add_dicts(dict1, dict2):
	partials = ['urban', 'rural', 'total']
	merged_dict = dict1
	for partial in partials:
		merged_dict[partial]['area'] = dict1[partial]['area'] + dict2[partial]['area']
		c1 = collections.Counter(dict1[partial]['population'])
		c2 = collections.Counter(dict2[partial]['population'])
		merged_dict[partial]['population'] = dict(c1 + c2)
	return merged_dict

def sub_dicts(dict1, dict2):
	partials = ['urban', 'rural', 'total']
	merged_dict = dict1
	for partial in partials:
		merged_dict[partial]['area'] = dict1[partial]['area'] - dict2[partial]['area']
		c1 = collections.Counter(dict1[partial]['population'])
		c2 = collections.Counter(dict2[partial]['population'])
		merged_dict[partial]['population'] = dict(c1 - c2)
	return merged_dict

telangana_dists = ['Adilabad', 'Hyderabad', 'Karimnagar', 'Khammam', 'Mahbubnagar', 'Medak', 'Nalgonda', 'Nizamabad', 'K.V.Rangareddy', 'Warangal']
census_states = json.load(open(path.join(data_dir, 'census', 'population_states.json'), 'r'))
census_states_dict = census_states['States']
census_dists = json.load(open(path.join(data_dir, 'census', 'population_districts.json'), 'r'))
census_dists_dict = census_dists['Districts']
telangana_dicts = list(map(lambda x: census_dists_dict[x], telangana_dists))

# add telangana with district totals
telangana_dict = telangana_dicts[0]
for td in telangana_dicts[1:]:
	telangana_dict = add_dicts(telangana_dict, td)

### fill total area value of states with sum of rural and urban areas
for key in census_states_dict.keys():
	total_area = census_states_dict[key]['rural']['area'] + census_states_dict[key]['urban']['area']
	census_states_dict[key]['total']['area'] = total_area

# remove telangana district totals from Andhra Pradesh
census_states_dict['Andhra Pradesh'] = sub_dicts(census_states_dict['Andhra Pradesh'], telangana_dict)
census_states_dict['Telangana'] = telangana_dict
census_states['States'] = census_states_dict
json.dump(census_states, open(path.join(data_dir, 'census', 'population_states.json'), 'w'))

### Change affiliation of Telangana districts
for td in telangana_dists:
	census_dists_dict[td]['state'] = 'Telangana'

census_dists['Districts'] = census_dists_dict
json.dump(census_dists, open(path.join(data_dir, 'census', 'population_districts.json'), 'w'))
### TODO: adapt district and statenames?