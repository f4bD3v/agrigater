### Call this script during creation of market collection?
# 2500 requests per day, 10 requests per second
import requests
import pandas as pd

def get_market_coordinates(pin, market, district, state):
	# put in hidden yaml config (don't commit)
	api_key = 'AIzaSyAKkDdjKy0odgtfGFsjkkGXDK8vWF_AeiQ'
	# put market name, district, state, country
	# postal code works when location name not given
	location = list(filter(lambda x: not pd.isnull(x), [pin, market, district, state, 'India']))
	address = ' ,'.join(location)
	# if there is road information in the address: extract it?
	#address = 'Market, Achanta, West Godavari'# Maruteru Koderu Road, ,
	# use market address? (attach state) if it is not in address) if that is not given, use market name, district and state
	url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address, api_key)

	r = requests.get(url)
	json = r.text
	print(r.text)
	# retrieve coordinates of first result entry
	coordinates = json['results'][0]['geometry']['location']
	print(coordinates)
	return coordinates

#coord = get_market_coordinates()