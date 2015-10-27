import os
from os import path
import pandas as pd
import glob
import json
### extract dict of crops with respective lists of varieties and use it to create a mapping for cleaning

### this should be enough to change pandas to blaze code for processing large files
## pandas to blaze: http://blaze-docs.readthedocs.org/en/latest/rosetta.html
data_dir = '../../../data'

def main():
	src_dir = path.join(data_dir, 'agmarknet/by_crop')
	init_dir = os.getcwd()
	os.chdir(src_dir)
	folders = glob.glob('*')
	csv_dir = os.getcwd()
	### TODO: run script on several cores by dividing list of folders into chunks ==> combine resulting dictionaries
	### --> not necessary
	comm_dict = {}
	for folder in folders:
		print(folders)
		os.chdir(path.join(csv_dir, folder))
		files = glob.glob('*_all.csv')
		#files = listdir(path.join(src_dir, folder))
		prev_commodity = None
		for file in files:
			print(file)
			commodity = file.split('_')[0]
			if prev_commodity != commodity:
				print(comm_dict)
				print(commodity)
				prev_commodity = commodity
			if not commodity in comm_dict:
				comm_dict[commodity] = {}
			if not 'variety' in comm_dict[commodity]:
				comm_dict[commodity]['variety'] = []
			if not 'grade' in comm_dict[commodity]:
				comm_dict[commodity]['grade'] = []
			df = pd.DataFrame.from_csv(file, index_col=None, header=None)	
			df = df.apply(lambda x: x.astype(str).str.strip(), axis=1)
			if df.shape[1] == 10:
				df.columns = ['Date', 'State', 'Market', 'Crop', 'Variety', 'Arrival', 'Grade', 'Min', 'Max', 'Modal']
				grade = list(filter(bool, df['Grade'].unique()))
				#if not grade in comm_dict[commodity]['grade']:
				comm_dict[commodity]['grade'] = comm_dict[commodity]['grade'] + grade
				comm_dict[commodity]['grade'] = list(set(comm_dict[commodity]['grade']))
			elif df.shape[1] == 11:
				df[[3,4,5]].apply(lambda x: ' '.join(x), axis=1)
				df.drop(4, axis=1, inplace=True)
				df.drop(5, axis=1, inplace=True)
				df.columns = ['Date', 'State', 'Market', 'Crop', 'Variety', 'Arrival', 'Min', 'Max', 'Modal']
			else:
				df.columns = ['Date', 'State', 'Market', 'Crop', 'Variety', 'Arrival', 'Min', 'Max', 'Modal']
			df['Variety'] = df['Variety'].apply(str).str.strip()

			variety = list(filter(bool, df['Variety'].unique()))
			#if not variety in comm_dict[commodity]['variety']:
			comm_dict[commodity]['variety'] = comm_dict[commodity]['variety'] + variety
			comm_dict[commodity]['variety'] = list(set(comm_dict[commodity]['variety']))

	os.chdir(init_dir)
	### NOTE: would need to open file
	json.dump(comm_dict, path.join(data_dir, 'commodities', 'commodity_dict.json'))

if __name__ == "__main__":
	main()