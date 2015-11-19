import sys
import os
from os import path
import glob
import blaze as bz
import odo
import pandas as pd
import json
import re
import time
from datetime import datetime

data_dir = '../../../data'

commodity_corrections = {
    'Pine Apple' : 'Pineapple',
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
    'Jowar(Sorgham)' : 'Sorghum',
    'Soyabean' : 'Soybean',
    'Paddy(Dhan)' : 'Paddy',
    'Bajra(Pearl Millet)' : 'Pearl Millet',
    'Barley (Jau)' : 'Barley',
    'Ragi (Finger Millet)' : 'Finger Millet',
    'Green Chilly' : 'Green Chilli',
    'Bhindi(Ladies Finger)' : 'Ladies Fingers'
}

# if name in parentheses contained in main name, remove parentheses with contents
### TODO: seems like the mapping does not work
commodity_name_mapping = {
    'Jack Fruit' : 'Jackfruit',
    'Mousambi' : 'Sweet Lemon',
    'Isabgul (Psyllium)' : 'Psyllium',
    'Sesamum(Sesame,Gingelly,Til)' : 'Sesame',
    'Gingelly Oil' : 'Sesame Oil',
    'Arecanut(Betelnut/Supari)' : 'Areca Nut',
    'Soanf' : 'Fennel',
    'Brinjal' : 'Eggplant',
    'Torchwood' : 'Sea Torchwood', #, 'Sea Amyris',
    'Karbuja' : 'Muskmelon',
    'Amorphophallus' : 'Elephant Foot Yam',
    'Thondekai' : 'Ivy Gourd',
    'Myrobalan' : 'Yellow Myrobalan',
    'Mahua' : 'Madhuca Longifolia',
    'Karamani' : 'Cow Pea',
    'Marasebu' : 'Pear',
    'Jau' : 'Barley',
    'Seemebadnekai' : 'Chayote Squash',
    'Colacasia' : 'Taro',
    'Gurellu' : 'Niger Seed',
    'Maragenasu' : 'Cassava',
    'Bore Hannu' : 'Jujube',
    'Rajgira' : 'Amaranth', ### ??
    'Marygold(Loose)' : 'Marigold', #--> variety Loose
    'Methi' : 'Fenugreek',
    'Chakotha' : 'Grapefruit',
    'Siddota' : 'Muskmelon',
    'Balekai' : 'Plantain',
    'Anjura' : 'Fig',
    'Toria' : 'Canola', # unclear
    'Raya' : 'Indian Mustard',
    'Suran' : 'Elephant Foot Yam', # Stink Lily
    # Elephant Yam (Suran)
    'Kakada' : 'Indian Jasmine',
    'Antawala' : 'Soapnut',# ???
    'Kacholam' : 'Aromatic Ginger',
    'Anthorium' : 'Anthurium',
    'Galgal(Lemon)' : 'Hill Lemon',
    'Marget' : 'Passion Flower Lady Margaret',
    'Matki' : 'Moth Bean',
    'Sajje' : 'Pearl Millet',
    'Tinda' : 'Indian Round Gourd',
    'Varagu' : 'Kodo Millet',
    'Antwala' : 'Soapnut',
    'Ridgeguard(Tori)' : 'Turai',
    'Turai' : 'Ridge Gourd',
    'Taramira' : 'Jamba',
    'Dhaincha' : 'Sesbania Cannabina',
    'Aster' : 'Indian Aster',
    'Chikoos(Sapota)' : 'Sapodilla',
    'Kanakambara' : 'Firecracker Flower',
    'Bengal Grams(Gram)' : 'Chickpea',
    'Black Grams (Urd Beans)' : 'Black Gram',
    'Arhar (Tur)' : 'Pidgeon Pea',
    'Red Grams' : 'Pidgeon Pea',
    'Green Grams(Moong)' : 'Mung Bean',
    'Green Gram(Dal)' : 'Green Dal',
    'Peas(Dry)' : 'Dried Peas'
}

### NOTE: paddy = harvested rice that has not been milled yet (outer layer is removed), grain separated from husk though

state_corrections = {
    'Orissa' : 'Odisha',
    'Chattisgarh' : 'Chhattisgarh',
    'Uttrakhand' : 'Uttarakhand'
}

def drop(expr, columns=()):
    return expr[[f for f in expr.fields if f not in columns]]

### TODO: process varieties
def extract_dicts(folder, files, commodity_dict, category_dict, market_commodity_dict):
    # extract commodities (+ category, market fields), categories, extract commodities by markets
    print(folder)
    for filename in files:
        print(filename)
        # commodity = filename.split('_')[0]
        # Have to use resource to discover URIs
        csvr = bz.resource(filename)
        num_col = len(bz.discover(csvr)[1].types)   
        ds = None
        if num_col == 10:
            ### TODO: dates are being misinterpreted: datetime => date?
            ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, category: ?string, commodity: ?string, variety: ?string, arrival: ?float64, min: ?float64, max: ?float64, modal: ?float64}")
        elif num_col == 11: 
            ds = bz.dshape("var * {date: datetime, state: ?string, market: ?string, category: ?string, commodity: ?string, variety: ?string, arrival: ?float64, grade: ?string, min: ?float64, max: ?float64, modal: ?float64}")
        else:
            ds = bz.discover(csvr)

        df = bz.Data(filename, dshape=ds)
        commodity = list(filter(bool, list(df['commodity'].distinct())))[0].strip()
        """
        if ext_commodity != commodity:
            print(commodity, ext_commodity)
            print('Commodity filename and content do not match!')
        """

        ### Categories
        if not category_dict[folder]:
            category_dict[folder] = [commodity]
        if not commodity in category_dict[folder]:
            category_dict[folder] = category_dict[folder] + [commodity]

        ### Commodities
        if not commodity in commodity_dict:
            commodity_dict[commodity] = {}
        if not 'grade' in commodity_dict:
            commodity_dict[commodity]['grade'] = []
        if not 'variety' in commodity_dict:
            commodity_dict[commodity]['variety'] =  []
        if not 'states' in commodity_dict:
            commodity_dict[commodity]['states'] = []
        if not 'markets' in commodity_dict:
            commodity_dict[commodity]['markets'] = []

        if not 'category' in commodity_dict[commodity]:
            commodity_dict[commodity]['category'] = [folder]
        if not folder in commodity_dict[commodity]['category']:
            commodity_dict[commodity]['category'] = commodity_dict[commodity]['category'] + [folder]

        if 'grade' in df.fields:
            ext_grades = list(map(lambda x: x.strip(), list(df['grade'].distinct())))
            grades = list(filter(bool, ext_grades))
            commodity_dict[commodity]['grade'] = commodity_dict[commodity]['grade'] + grades
            commodity_dict[commodity]['grade'] = list(set(commodity_dict[commodity]['grade']))

        ext_varieties = list(map(lambda x: x.strip(), list(df['variety'].distinct())))
        varieties = list(filter(bool, ext_varieties))
        commodity_dict[commodity]['variety'] = commodity_dict[commodity]['variety'] + varieties
        commodity_dict[commodity]['variety'] = list(set(commodity_dict[commodity]['variety']))

        states = list(df['state'].distinct())
        commodity_dict[commodity]['states'] = commodity_dict[commodity]['states'] + states
        commodity_dict[commodity]['states'] = list(set(commodity_dict[commodity]['states']))

        # add market field in commodities dict
        markets = list(df['market'].distinct())
        commodity_dict[commodity]['markets'] = commodity_dict[commodity]['markets'] + markets
        commodity_dict[commodity]['markets'] = list(set(commodity_dict[commodity]['markets']))

        # market_commodity_dict needs group by or split operation?
        # simply extract a unique set of market names, loop over it and append commodity + 
        for market in markets:
            if not market in market_commodity_dict:
                market_commodity_dict[market] = [commodity]
            else:
                market_commodity_dict[market] = list(set(market_commodity_dict[market] + [commodity]))

    return (commodity_dict, category_dict, market_commodity_dict)

### TODO - os.remove() to remove a file
### NOTE:
# - batch => tested
# - online => todo
def clean(files, mode, commodity_corrections, commodity_name_mapping):
    if mode == 'batch':
        #commodities = list(set(list(map(lambda x: x.split('_')[0], files))))
        #for commodity in commodities:
        for filename in files:
            #commodity = commodity.replace('(', '\(').replace(')', '\)')
            #filename = '{}_stacked.csv'.format(commodity)
            # Have to use resource to discover URIs
            csvr = bz.resource(filename)
            num_col = len(bz.discover(csvr)[1].types)   
            print(num_col)
            ds = None
            if num_col == 10:
                ds = bz.dshape("var * {date: ?string, state: ?string, market: ?string, category: ?string, commodity: ?string, variety: ?string, arrival: ?float64, min: ?float64, max: ?float64, modal: ?float64}")
            elif num_col == 11: 
                ds = bz.dshape("var * {date: ?string, state: ?string, market: ?string, category: ?string, commodity: ?string, variety: ?string, arrival: ?float64, grade: ?string,  min: ?float64, max: ?float64, modal: ?float64}")
            else:
                ds = bz.discover(csvr)
            d = bz.Data(filename, dshape=ds)
            ### Use Dask if Data loads index_col and/or header: 
            # http://stackoverflow.com/questions/32716093/how-do-i-read-tabulator-separated-csv-in-blaze
            ### Fixes issue with two added months on date
            d = bz.transform(d, date=d.date.map(lambda x: datetime.strptime(x, '%d/%m/%Y').date(), 'date'))

            commodity = list(d.commodity.distinct())[0]
            varieties = list(d.variety.distinct())
            ### Removing unnecessary varieties
            ### TODO: test effectiveness
            if [commodity, 'Other'] == varieties:
                d = bz.transform(d, variety=d.variety.map(lambda x: commodity, 'string'))

            ### TODO: merge related filenames

            d = bz.transform(d, commodity=d.commodity.map(lambda x: commodity_corrections[x] if x in commodity_corrections else x, 'string'))
            d = bz.transform(d, commodityTranslated=d.commodity.map(lambda x: commodity_name_mapping[x] if x in commodity_name_mapping else x, 'string'))
            d = bz.transform(d, state=d.state.map(lambda x: state_corrections[x] if x in state_corrections else x, 'string'))
            ### TODO: there must be a better solution for this
            #for key, val in market_corrections.items():
            # d = bz.transform(d, market=d.market.map(lambda x: re.sub(key, val, x), 'string'))
            cleaned_fn = filename.replace('.csv', '_cleaned.csv')# {}_stacked_cleaned.csv'.format(commodity)
            print(cleaned_fn)
            if not path.isdir('cleaned'):
                os.makedirs('cleaned')
            outpath = path.join('cleaned', cleaned_fn)
            print(outpath)
            if path.isfile(outpath):
                os.remove(outpath)
            odo.odo(d, outpath)
            #df_cleaned = odo.odo(d, pd.DataFrame)
            #df_cleaned.to_csv(outpath, index=False)
            # df = pd.DataFrame.from_csv('cleaned/Tea_stacked_cleaned.csv', header=False, index_col=None)
            # df['year'] = df['date'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").year)
            # df['month'] = df['date'].apply(lambda x: datetime.strptime(x, "%d/%m/%Y").month)
            
            # os.remove(filename)
    else:
        for filename in files:
            df = pd.DataFrame.from_csv(filename, header=False, index_col=False)
            num_col = len(df.columns)
            if num_col == 10:
                df.columns = ['date', 'state', 'market', 'category', 'commodity', 'variety', 'arrival', 'min', 'max', 'modal']
            elif num_col == 11: 
                df.columns = ['date', 'state', 'market', 'category', 'commodity', 'variety', 'arrival', 'grade', 'min', 'max', 'modal']

            df['state'] = df['state'].replace(commodity_corrections, replace=True)
            df['commodity'] = df['commodity'].replace(commodity_corrections, replace=True)
            df['commodity'] = df['commodity'].replace(commodity_name_mapping, replace=True)
            cleaned_fn = '{}_cleaned.csv'.format(filename.rstrip('.csv'))
            if not path.isdir('cleaned'):
                os.makedirs('cleaned')
            outpath = path.join('cleaned', cleaned_fn)
            if path.isfile(outpath):
                os.remove(outpath)
            df.to_csv(cleaned_fn, index=False)
        ### TODO: use blaze to load commodity files, process them and save to desk cleaned version
    #### "online" --> use pandas
    return

"""
NOTE: this method operates in a ${category} folder where files are stored as ${commodity}_${date}.csv
TODO: change this method to stack from 
    by_date_and_commodity/${category}/${commodity}/integrated/*localized.csv
    to by_commodity/${category}/${commodity}/integrated/*stacked_localized.csv
"""
def merge(folder, files, mode, stage='integrated', replace=False):
    ### "online" --> do not remove stacked files
    print(folder)
    print('Entering merging..')
    if mode == 'batch':
        replace = True 

    commodities = list(set(list(map(lambda x: x.split('_')[0], files))))
    print(commodities)
    """
    NOTE:
    - in "online" mode: clean BEFORE "append to stack" (limited files to clean) => TODO: test
    - in batch mode (many files): clean AFTER stacking => tested

    dealing with filenames:
        - correct filenames according to dictionary first
        - ( -> _
        - ) -> ?
        - ' ' -> -
        - '-_' -> _
        - '_-' -> _
    """
    for commodity in commodities:
        # re.sub(r'\s+', '', commodity)
        orig_commodity = commodity.replace(' ', '\ ').replace('(', '\(').replace(')', '\)')
        #orig_commodity_uncleaned = orig_commodity
        #orig_commodity = orig_commodity.replace('\\', '')
        ### TODO: move this to cleaning?
        if commodity in commodity_corrections:
            commodity = commodity_corrections[commodity]
        commodity = commodity.replace('(', '_')
        commodity = commodity.replace(')', '')
        commodity = commodity.replace(' ', '-').replace('_-', '_').replace('-_', '_')
        commodity = re.sub('-+', '-', commodity)
        #print(commodity)
        #commodity = commodity.replace('(', '\(').replace(')', '\)')
        target_dir = os.getcwd().replace('by_date_and_commodity', 'by_commodity')

        #print(target_dir)
        if not path.isdir(target_dir):
            os.makedirs(target_dir)
        target = '\"{1}/{0}_stacked.csv\"'.format(commodity, target_dir)
        ### NOTE: in batch mode files are stacked in by_commodity/${category}/${commodity} and then processed towards integrated/
        if replace:
            #quick_fix = "/bin/bash -c \"rename -n 's/{0}_(.*)\.csv/{1}_$1\.csv/' {0}*.csv \"".format(orig_commodity_uncleaned, orig_commodity)
            #print(quick_fix)
            #os.system(quick_fix)
            print('Stacking all {} files ..'.format(commodity))
            if path.isfile(target):
                os.system('/bin/bash -c \"rm {}\"'.format(target))
            # "Bottle gourd"
            command = '/bin/bash -c \"cat {0}_*.csv > {1}\"'.format(orig_commodity, target)
            os.system(command)
        ### TODO: in online mode files are first processed to by_date_and_commodity/${cat}/${comm}/integrated and then appended to 
        # the already existing file stacks in by_commodity/${cat}/${comm}/integrated
        # functionality can be changed back to updating only cleaned files with the 'status' variable
        elif not replace:
            target_dir = target_dir.replace('cleaned', 'integrated')
            print('Appending incoming {} files ..'.format(commodity))
            ## should I append to stacked ones too?
            curr_dir = os.getcwd()
            os.chdir(target_dir)
            #os.chdir('../../by_date_and_commodity/{}'.format(folder))
            files = glob.glob('*{}.csv'.format(stage)) 
            if files:
                command = '/bin/bash -c \"cat {0}_*{3}.csv >> {1}/{2}_stacked_{3}.csv\"'.format(orig_commodity, target_dir, commodity, stage)
                os.system(command)
            os.chdir(curr_dir)
    return

def main(task, mode):
    print(task)
    print(mode)
    comm = json.load(open(path.join(data_dir, 'commodities', 'commodity_corrections_mappings.json'), 'r'))
    commodity_corrections = comm['commodity_corrections']
    commodity_name_mapping = comm['commodity_name_mapping']
    print('commodity name mapping: ', commodity_name_mapping)
    src_dir = path.join(data_dir, 'agmarknet/by_date_and_commodity')
    init_dir = os.getcwd()
    os.chdir(src_dir)
    folders = glob.glob('*')
    csv_dir = os.getcwd()
    commodity_dict = {}
    category_dict = {}
    market_commodity_dict = {}
    title = ''
    start = time.time()
    for folder in folders:
        outpath = path.join(csv_dir, folder)
        if path.isfile(outpath):
            continue
        category_dict[folder] = []

        os.chdir(outpath)
        if task == 'merge':
            files = glob.glob('*.csv')
            merge(folder, files, mode)
            title = '{} merging'.format(mode)
        elif task == 'clean':   
            ### what to do for cleaning ==> replace files with _cleaned attachment
            title = '{} cleaning'.format(mode)
            if mode == 'batch':
                curr_dir = os.getcwd()
                stack_dir = curr_dir.replace('by_date_and_commodity', 'by_commodity')
                print(stack_dir)
                os.chdir(stack_dir)
                #os.chdir('../../by_commodity/{}'.format(folder))
                files = glob.glob('*stacked.csv')
                print(files)
                clean(files, mode, commodity_corrections, commodity_name_mapping)
                os.chdir(curr_dir)
            else:
                clean(files, mode)  
        elif task == 'extract':
            ### TODO: extract dicts from integrated data? => no, for now save all data stages on disk
            title = 'dictionary extraction'
            curr_dir = os.getcwd()
            stack_dir = curr_dir.replace('by_date_and_commodity', 'by_commodity')
            stack_dir = path.join(curr_dir, 'cleaned')
            print(stack_dir)
            os.chdir(stack_dir)
            files = glob.glob('*stacked_cleaned.csv')
            if mode != 'batch':
                print('Extraction works in batch mode only')
                sys.exit()
            ### NOTE: batch mode only
            commodity_dict, category_dict, market_commodity_dict = extract_dicts(folder, files, commodity_dict, category_dict, market_commodity_dict)
            os.chdir(curr_dir)

    os.chdir(init_dir)
    if task == 'extract':
        ### save dictionaries
        json.dump(commodity_dict, open(path.join(data_dir, 'commodities', 'commodity_dict.json'), 'w'))
        not_other = [item for key, item in commodity_dict.items() if 'Other' not in item['category']]   
        json.dump(not_other, open(path.join(data_dir, 'commodities', 'commodity_dict_without_other.json'), 'w'))
        json.dump(category_dict, open(path.join(data_dir, 'commodities', 'category_dict.json'), 'w'))
        json.dump(market_commodity_dict, open(path.join(data_dir, 'markets', 'market_commodity_dict.json'), 'w'))

    end = time.time()
    unit = 'minutes'
    elapsed = int((end - start) // 60)
    if elapsed > 60:
        elapsed = elapsed // 60
        unit = 'hours'
    print('{2} completed in {0} {1}!'.format(elapsed, unit, title))
    return

if __name__ == "__main__":
    task = ['clean', 'merge', 'extract']
    mode = ['batch', 'online']
    ### TODO: use argparse!!!   
    if len(sys.argv) > 1 and sys.argv[1] in task and sys.argv[2] in mode:
        main(*sys.argv[1:])
    else:
        print('usage: {} (clean|merge) (batch|online)'.format(sys.argv[0]))