import yaml
from mongo_handler import MongoDB
import numpy as np
import pandas as pd

config = yaml.load(open('../db/db_config.yaml', 'r'))
db = MongoDB(config['meteordb'], config['address'], config['meteorport'])

coll_name = "market.wheat"

### NOTE: "$ne": np.nan works!!! ###
pipeline = [
    { "$match" : {
        ### TODO: dont think np.nan works here, find another way
        #, "$ne": np.nan
        "commodityTonnage" : { "$exists": True, "$ne": 0, "$ne": None, "$ne": np.nan }
        #"price" : {"$ne" : np.nan }
        }
    },
    { "$limit" : 1000 },
    { "$project" : {
        "_id" : 0,
        "commodityTonnage": 1
     }}
]

res = db.db[coll_name].aggregate(pipeline, allowDiskUse=True)
print(list(res))
