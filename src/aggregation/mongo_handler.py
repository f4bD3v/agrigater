import pymongo
from pymongo import MongoClient
from pymongo.errors import *
from yaml import load

import re
import sys

# note: BSON strings are utf-8 encoded --> use python 3
class MongoDB:
	# mongodb will lazily create db and collections
    def __init__(self, db_name, address, port):
        self.client = MongoClient(address, port)
        self.db = self.client[db_name]
        return

    # insert states
    # insert districts
    # insert markets

    # create commodity collections by level
    	# - market_${commodities}
    	# - district_${commodities}
    	# - state_${commodities}
    def insert(self, coll_name, obj):
        try:
            self.db[coll_name].insert(obj, continue_on_error = False)
        except DuplicateKeyError:
            print('DuplicateKeyError due to mutliple insertions of users! Continue_on_error flag set to True')
        except DocumentTooLarge:
            print(obj)
        return

    def bulk_insert(self, coll_name, obj_list):
        try:
            self.db[coll_name].insert_many(obj_list, continue_on_error = False)
        except DuplicateKeyError:
            print('DuplicateKeyError due to mutliple insertions of users! Continue_on_error flag set to True')
        except DocumentTooLarge:
            print(obj)
        return

    def fetch(self, collname, query, islice):
        if isinstance(islice, tuple):
            res = self.db[collname].find(query)[islice[0]: islice[1]]
        else:
            res = self.db[collname].find(query)
        return res

    def fetch_sorted(self, collname, query, sort_field, order):
        if order == 'ASC':
            return self.db[collname].find(query).sort(sort_field, pymongo.ASCENDING)
        elif order == 'DESC':
            return self.db[collname].find(query).sort(sort_field, pymongo.DESCENDING)

    def fetch_sorted_limited(self, collname, sort_field, order, l):
        print(pymongo.ASCENDING)
        if order == 'ASC':
            res = self.db[collname].find().sort(sort_field, pymongo.ASCENDING).limit(l)
        elif order == 'DESC':
            res = self.db[collname].find().sort(sort_field, pymongo.DESCENDING).limit(l)
        return res  


# Running Mongo: mongod --dbpath ~/mongodb/data/db/
def main():
    config = load(open('../db/db_config.yaml', 'r'))
    mongodb = MongoDB(config['db'], config['address'], config['port'])
    state_dict = {}
    mongodb.insert('states', state_dict)
    mongodb.insert('districts', state_dict)
    mongodb.insert('markets', state_dict)
    return

if __name__ == "__main__":
	main()
