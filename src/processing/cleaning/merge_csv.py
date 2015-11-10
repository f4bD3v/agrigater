#!/usr/bin/env python3
import os
from os import path
import glob

data_dir = '../../../data'

def merge_agmarknet(replace = False):
	working_dir = path.join(data_dir, 'agmarknet')
	os.chdir(working_dir)
	wdir = os.getcwd()
	folders = os.listdir(wdir)
	for folder in folders:
		if not os.path.isdir(folder):
			continue
		print(folder)
		os.chdir(path.join(wdir, folder))
		print(os.getcwd())
		files = glob.glob('*csv')
		commodities = list(set(list(map(lambda x: x.split('_')[0], files))))
		for commodity in commodities:
			if os.path.isfile('{}_all.csv'.format(commodity)) and replace:
				os.system('/bin/bash -c \"rm {}_all.csv\"'.format(commodity).replace('(', '\(').replace(')', '\)'))
			### this doesn't work!	
			### TODO: use folder variable
			os.system('/bin/bash -c \"cat {0}_*csv > ../../by_date_and_commodity/{1}/{0}_all.csv\"'.format(commodity, folder).replace('(', '\(').replace(')', '\)'))
		os.chdir(wdir)
		# Delete all files that do not contain '_all.csv' pattern
		# find . -type f ! -name '*_all.csv' -delete
	return

def main():
	merge_agmarknet()
	return

if __name__ == "__main__":
	main()	