### Call this script during creation of market collection?
# 2500 requests per day, 10 requests per second
import requests
import pandas as pd
import numpy as np
import os
from os import path
import glob
import json
import re
import math
import logging
import time

data_dir = '../../../data'

def get_market_coordinates(pin, market, district, state, first=True):
	# put in hidden yaml config (don't commit)
	api_key = 'AIzaSyAKkDdjKy0odgtfGFsjkkGXDK8vWF_AeiQ'
	# put market name, district, state, country
	# postal code works when location name not given
	### TODO: if city field available, use instead of market name
	if str(market) == 'nan':
		raise Exception('Market name not a value!')
	"""
	if str(district) != 'nan' and district in market:
		market = market.replace(district, '').strip().lstrip('(').rstrip(')')
		if not market:
			market = district
	"""
	loc_list = []
	if str(pin) == 'nan':
		loc_list = [market, district, state, 'India']
	else:
		loc_list = [str(pin), market, district, state, 'India']

	location = list(filter(lambda x: not pd.isnull(x), loc_list))
	### Can find exact location by adding 'market yard', but will not always work because ambiguities
	address = ', '.join(location)
	print(address)
	# if there is road information in the address: extract it?
	#address = 'Market, Achanta, West Godavari'# Maruteru Koderu Road, ,
	# use market address? (attach state) if it is not in address) if that is not given, use market name, district and state
	url='https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}'.format(address, api_key)

	r = requests.get(url)
	geojson = json.loads(r.text)
	# retrieve coordinates of first result entry
	if geojson['status'] == 'OK':
		print(len(geojson['results']))
		coordinates = geojson['results'][0]['geometry']['location']
	else: 
		if first:
			market = re.sub('\(.*\)', '', market).strip()
			coordinates, geojson = get_market_coordinates(pin, market, district, state, False)
		else:
			coordinates = None
	print(coordinates)
	return (coordinates, geojson)

def add_coordinates(row, logger):
	pin = row['Pin']
	if not math.isnan(pin):
		pin = int(pin)
	coordinates, geojson = get_market_coordinates(pin, row['Market'], row['District'], row['State'])
	logger.info('Retrieved following geocode json for market location...')
	logger.info(geojson)
	res_num = len(geojson['results'])
	logger.info('Number of results: {}'.format(res_num))
	logger.info('\n')
	series = pd.Series([row['State'], row['Market_Cleaned'], coordinates['lat'], coordinates['lng']], ['State', 'Market', 'Lat', 'Lng'])
	time.sleep(2)
	return series

def main():
	logger = logging.getLogger('markets_geolocate')
	logger.setLevel(logging.INFO)
	fh = logging.FileHandler('geolocate_markets.log')
	fh.setLevel(logging.INFO)
	logger.addHandler(fh)
	curr_dir = os.getcwd()
	src_dir = path.join(data_dir, 'markets')
	os.chdir(src_dir)
	files = glob.glob('*localized.csv')
	coordinate_dfs = []
	for file in files:
		df = pd.DataFrame.from_csv(file, index_col=None)
		df = df.replace('*', np.nan)
		coordinate_df = df.apply(add_coordinates, axis=1, args=[logger])
		coordinate_dfs.append(coordinate_df)
		time.sleep(180)
	os.chdir(curr_dir)
	coordinate_df = pd.concat(coordinate_dfs)
	pd.DataFrame.to_csv(coordinate_df, path.join(data_dir, 'admin/geo', 'market_coordinates.csv'))
	return

if __name__ == "__main__":
	main()	