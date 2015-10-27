"""
	- drop all documents: result = db.collection.delete_many({})
	- drop a collection: db.collection.drop()
	--> simpler just to drop collection
"""

def drop_commodities(db, logger):
	cat_comm = json.load(open('comm_by_category.json', 'r'))
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