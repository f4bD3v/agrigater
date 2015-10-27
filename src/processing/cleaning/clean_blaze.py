import os
from os import path
import pandas as pd
import glob
import json
import blaze as bz, odo
### extract dict of crops with respective lists of varieties and use it to create a mapping for cleaning

spelling_dict = {
	'Soanf' : 'Saunf',
	'Amaranthus' : 'Amaranth',
	'Lilly' : 'Lily',
	'Amphophalus' : 'Amorphophallus',
	'Cornation' : 'Carnation',
	'Colacasia' : 'Colocasia',
	'Maragensu' : 'Maragenasu',
	'Mashroom' : 'Mushroom',
	'Borehannu' : 'Bore Hannu',
	'Rajgir' : 'Rajgira',
	'Cane' : 'Sugarcane',
	'Suram' : 'Suran',
	'Cinamon' : 'Cinnamon',
	'Mataki' : 'Matki',
	'Kankambra' : 'Kanakambara',
	'Pears' : 'Pear',
	'Ajwan' : 'Ajwain', # Bishop's Weed
	'Cardamoms' : 'Cardamom',
	'Lentil(Masur)' : 'Lentil(Masoor)',
	'Antawala' : 'Antwala',
	'Cucumbar' : 'Cucumber',
	'Astera' : 'Aster',
	'Gur(Jaggery)' : 'Jaggery',
	'Pigs' : 'Pig',
	'BOP' : 'Bird of Paradise',
	'Jowar(Sorgham)' : 'Sorghum'
}

### this should be enough to change pandas to blaze code for processing large files
## pandas to blaze: http://blaze-docs.readthedocs.org/en/latest/rosetta.html
data_dir = '../../../data'

### write script that assigns or deletes headers
def quick_fix():
	src_dir = path.join(data_dir, 'agmarknet/by_commodity')
	init_dir = os.getcwd()
	os.chdir(src_dir)
	folders = glob.glob('*')
	csv_dir = os.getcwd()
	for folder in folders:
		### loading whole directory faster, but may pose problems due to different amount of columns
		#df = Data(path.join(csv_dir, folder, '*_all.csv'))
		os.chdir(path.join(csv_dir, folder))
		files = glob.glob('*_all.csv')
		for file in files:
			pre_df = Data(path.join(csv_dir, folder, file), has_header=False)
			num_col = len(pre_df.columns)	
			print(num_col, file)
			if num_col == 11:
				df = pd.DataFrame.from_csv(file, index_col=None, header=None)	
				#df = df.apply(lambda x: x.astype(str).str.strip(), axis=1)
				df[[3,4,5]].apply(lambda x: ' '.join(x), axis=1)
				df.drop(4, axis=1, inplace=True)
				df.drop(5, axis=1, inplace=True)
				df.columns = ['Date', 'State', 'Market', 'Commodity', 'Variety', 'Arrival', 'Min', 'Max', 'Modal']
				pd.DataFrame.to_csv(df, path.join(csv_dir, folder, file), header=False, index=False)
				## odo(df, 'output.csv', ?header=False)
				# save with pandas
	os.chdir(init_dir)
	return

def clean_csvs():
	def correct_commodities():
	src_dir = path.join(data_dir, 'agmarknet/by_commodity')
	init_dir = os.getcwd()
	os.chdir(src_dir)
	folders = glob.glob('*')
	csv_dir = os.getcwd()
	for folder in folders:
		os.chdir(path.join(csv_dir, folder))
		files = glob.glob('*_all.csv')
		for file in files:
			csvr = odo.resource(path.join(csv_dir, folder, file))  # Have to use resource to discover URIs
			num_col = len(odo.discover(csvr)[1].types)	
			ds = None
			if num_col == 9:
				ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?string, min: ?string, max: ?string, modal: ?string}")
			elif num_col == 10:	
				ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?string, grade: ?string, min: ?string, max: ?string, modal: ?string}")
			else:
				ds = odo.discover(csvr)

			d = bz.Data(path.join(csv_dir, folder, file), dshape=ds)
			if num_col == 10:
				d = bz.transform(d, grade=d.grade.map(lambda x: x.strip(), 'string'))

			d = bz.transform(d, commodity=d.commodity.map(lambda x: x.strip(), 'string'))
			d = bz.transform(d, commodity=d.commodity.map(lambda x: spelling_dict[x] if x in spelling_dict else x, 'string'))
			d = bz.transform(d, state=d.state.map(lambda x: x.strip(), 'string'))
			d = bz.transform(d, market=d.market.map(lambda x: x.strip(), 'string'))

	return

def correct_commodities():
	src_dir = path.join(data_dir, 'agmarknet/by_commodity')
	init_dir = os.getcwd()
	os.chdir(src_dir)
	folders = glob.glob('*')
	csv_dir = os.getcwd()
	for folder in folders:
		os.chdir(path.join(csv_dir, folder))
		files = glob.glob('*_all.csv')
		for file in files:
			csvr = odo.resource(path.join(csv_dir, folder, file))  # Have to use resource to discover URIs
			num_col = len(odo.discover(csvr)[1].types)	
			ds = None
			if num_col == 9:
				ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?string, min: ?string, max: ?string, modal: ?string}")
			elif num_col == 10:	
				ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?string, grade: ?string, min: ?string, max: ?string, modal: ?string}")
			else:
				ds = odo.discover(csvr)

			d = bz.Data(path.join(csv_dir, folder, file), dshape=ds)
			print(d.dshape.measure)
			d = bz.transform(d, commodity=d.commodity.map(lambda x: x.strip(), 'string'))
			d = bz.transform(d, commodity=d.commodity.map(lambda x: spelling_dict[x] if x in spelling_dict else x, 'string'))
			print(d.dshape.measure)
			print(list(bz.compute(d.commodity)))
	os.chdir(init_dir)

def extract_commodities():
	src_dir = path.join(data_dir, 'agmarknet/by_commodity')
	init_dir = os.getcwd()
	os.chdir(src_dir)
	folders = glob.glob('*')
	csv_dir = os.getcwd()
	commodity_names = {}
	category_names = {}
	for folder in folders:
		### loading whole directory faster, but may pose problems due to different amount of columns
		#df = Data(path.join(csv_dir, folder, '*_all.csv'))
		category_names[folder] = []
		os.chdir(path.join(csv_dir, folder))
		files = glob.glob('*_all.csv')
		for file in files:
			commodity = file.split('_')[0]
			if not category_names[folder]:
				category_names[folder] = [commodity]
			if not commodity in category_names[folder]:
				category_names[folder] = category_names[folder] + [commodity]
			if not commodity in commodity_names:
				commodity_names[commodity] = {}
			print(commodity, commodity_names[commodity], folder)
			if not 'category' in commodity_names[commodity]:
				commodity_names[commodity]['category'] = [folder]
			elif not commodity_names[commodity]['category']:
				commodity_names[commodity]['category'] = [folder]
			if not folder:
				print(" fuck this")
				break
			### How can the category field be None?
			print(commodity, commodity_names[commodity], folder)
			if not folder in commodity_names[commodity]['category']:
				commodity_names[commodity]['category'] = commodity_names[commodity]['category'] + [folder]
			if not 'grade' in commodity_names:
				commodity_names[commodity]['grade'] = []
			if not 'variety' in commodity_names:
				commodity_names[commodity]['variety'] = []
			if not 'states' in commodity_names:
				commodity_names[commodity]['states'] = []
			#print(compute(df.head(5)))
			#df = Data(path.join(csv_dir, folder, file), delimiter=", ")
			#print(compute(df.head()))
			### TODO: Need to make sure cells are stripped of whitespace!
			csvr = resource(path.join(csv_dir, folder, file))  # Have to use resource to discover URIs
			#print(discover(csvr))
			#dshape("var * {name: string, balance: int64}")
			num_col = len(discover(csvr)[1].types)	
			### OR: len(dict(discover(csvr)[1].fields).keys()))
			### TODO: order may not be valid anymore
			ds = None
			if num_col == 9:
				ds = dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?float64, min: ?float64, max: ?float64, modal: ?float64}")
			elif num_col == 10:	
				ds = dshape("var * {date: datetime, state: ?string, market: ?string, commodity: ?string, variety: ?string, arrival: ?float64, grade: ?string, min: ?float64, max: ?float64, modal: ?float64}")
			else:
				ds = discover(csvr)

			### DOESN'T WORK, BECAUSE DATA FORMAT MESSED UP
			#df = odo(path.join(csv_dir, folder, file), pd.DataFrame, dshape=ds)
			df = Data(path.join(csv_dir, folder, file), dshape=ds)

			### NOTE: have to do map strip because can't specify delimiter=", ", whitespace caused by creating csv file manually (', '.join(list))
			if 'grade' in df.fields:
				ext_grades = list(map(lambda x: x.strip(), list(df['grade'].distinct())))
				grades = list(filter(bool, ext_grades))
				commodity_names[commodity]['grade'] = commodity_names[commodity]['grade'] + grades
				commodity_names[commodity]['grade'] = list(set(commodity_names[commodity]['grade']))

			ext_commodity = list(filter(bool, list(df['commodity'].distinct())))[0].strip()
			if ext_commodity != commodity:
				print(commodity, ext_commodity)
				print('Commodity filename and content do not match!')
			ext_varieties = list(map(lambda x: x.strip(), list(df['variety'].distinct())))
			varieties = list(filter(bool, ext_varieties))
			commodity_names[commodity]['variety'] = commodity_names[commodity]['variety'] + varieties
			commodity_names[commodity]['variety'] = list(set(commodity_names[commodity]['variety']))
			states = list(df['state'].distinct())
			commodity_names[commodity]['states'] = commodity_names[commodity]['states'] + states
			commodity_names[commodity]['states'] = list(set(commodity_names[commodity]['states']))
			#print(compute(df['variety'].distinct()))
			#ds = dshape("var * {when: datetime, a: ?int64, b: ?int64, c: float64}")
			### TODO: learn how to create json
			#odo(df, '...json')
	os.chdir(init_dir)
	print(commodity_names)
	json.dump(json.dumps(commodity_names), open(path.join(data_dir, 'commodities', 'commodity_dict.json'), 'w'))
	non_other = [item for key, item in commodity_names.items() if 'Other' not in item['category']]	
	json.dump(json.dumps(non_other), open(path.join(data_dir, 'commodities', 'commodity_dict_without_other.json'), 'w'))
	json.dump(json.dumps(category_names), open(path.join(data_dir, 'commodities', 'category_dict.json'), 'w'))
	return

def main():
	#quick_fix()
	correct_commodities()
	#extract_commodities()

if __name__ == "__main__":
	main()