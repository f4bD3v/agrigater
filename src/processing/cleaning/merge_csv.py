#!/usr/bin/env python3
import os
from os import path
import glob

data_dir = '../../../data'

def merge_agmarknet():
	working_dir = path.join(data_dir, 'agmarknet/csv')
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
			if os.path.isfile('{}_all.csv'.format(commodity)):
				os.system('/bin/bash -c \"rm {}_all.csv\"'.format(commodity).replace('(', '\(').replace(')', '\)'))
			### this doesn't work!	
			os.system('/bin/bash -c \"cat {0}_*csv > {0}_all.csv\"'.format(commodity).replace('(', '\(').replace(')', '\)'))
		os.chdir(wdir)
	return

def main():
	merge_agmarknet()
	return

if __name__ == "__main__":
	main()	