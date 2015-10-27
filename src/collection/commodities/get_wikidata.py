import requests
import json
from os import path

data_dir = '../../../data'

comm_json = json.load(open(path.join(data_dir, 'commodities', 'commodity_dict.json'), 'r'))
comm_dict = json.loads(comm_json)
comms = comm_dict.keys()

url = 'http://en.wikipedia.org/w/api.php'

# http://stackoverflow.com/questions/8555320/is-there-a-clean-wikipedia-api-just-for-retrieve-content-summary/18504997#18504997
desc_params = {
	'action' : 'query',
	'format' : 'json',
	'prop' : 'extracts',
	'exintro' : '',
	'explaintext' : '',
	'titles' : None
}

thumb_params = {
	'action' : 'query',
	'format' : 'json',
	'prop' : 'pageimages',
	'pithumbsize' : 100,
	'titles' : None
}


comms = ['Cinnamon', 'Sugarcane', 'Mushroom', 'Cucumber']

for comm in comms:
	desc_params['titles'] = comm
	r = requests.get(url, params = desc_params)
	### TODO: get description & image
	data = r.json()
	print(data)
	thumb_params['titles'] = comm
	r = requests.get(url, params = thumb_params)
	data = r.json()
	print(data)
