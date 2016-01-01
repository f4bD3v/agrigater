import yaml
import json
from mongo_handler import MongoDB
from os import path
"""
    - drop all documents: result = db.collection.delete_many({})
    - drop a collection: db.collection.drop()
    --> simpler just to drop collection
"""

data_dir = '../../../data'

### THIS SCRIPT IS FOR AUTOMATION, FOR QUICK PROBLEM FIXING USE DROP.JS
def drop_commodities(db):
    ### TODO: load selected commodities here and prepend commodities tag
    cat_comm = json.load(open(path.join(data_dir, 'commodities', 'selected_commodities.json'))) 
    ### TODO: what to drop among the following? 'commodity_'comm'_variety, 'commodity_'comm, or 'commodity_state'_comm, 'commodity_district'_comm

    for cat in cat_comm.keys():
        if cat == 'Cereals':
            for comm in cat_comm[cat]:
                coll_name = 'market.'+comm.replace(" ", ".").lower()
                print(coll_name)
                db.db[coll_name].drop()
    return

def rename_collections(db):
    coll_names = db.db.collection_names(False)
    print(coll_names)
    for coll_name in coll_names:
        if "_" in coll_name:
            db.db[coll_name].rename(coll_name.replace("_", "."))
    return

def main():
    config = yaml.load(open('../../db/db_config.yaml', 'r'))
    db = MongoDB(config['meteordb'], config['address'], config['meteorport'])
    print(db)
    print(db.db)
    # TODO: choose what to drop depending on input: (commodity|market|state|district)
    #drop_commodities(db)
    #rename_collections(db)
    drop_commodities(db)
    return

if __name__ == "__main__":
    main()
