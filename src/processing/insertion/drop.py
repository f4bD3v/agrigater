"""
	- drop all documents: result = db.collection.delete_many({})
	- drop a collection: db.collection.drop()
	--> simpler just to drop collection
"""

### THIS SCRIPT IS FOR AUTOMATION, FOR QUICK PROBLEM FIXING USE DROP.JS
def drop_commodities(db, logger):
    ### TODO: load selected commodities here and prepend commodities tag
    cat_comm = json.load(open(path.join(data_dir, 'commodities', 'selected_commodities.json'))) 
    ### TODO: what to drop among the following? 'commodity_'comm'_variety, 'commodity_'comm, or 'commodity_state'_comm, 'commodity_district'_comm
	cats = cat_comm.keys()
	for cat in cats:
		db[cat].drop()
		logger.info('dropped category collection: '+cat)
	return

def main():
    config = yaml.load(open('../../db/db_config.yaml', 'r'))
    logger = logging.getLogger('db_drop')
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('drop.log')
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)
    db = MongoDB(config['db'], config['address'], config['port'])
    # TODO: choose what to drop depending on input: (commodity|market|state|district)
    drop_commodities(db, logger)
    return

if __name__ == "__main__":
	main()