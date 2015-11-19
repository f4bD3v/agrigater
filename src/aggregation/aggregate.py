import bson
import numpy as np
from bson.son import SON
from os import path
import json
import yaml
from mongo_handler import MongoDB

data_dir = '../../data'
### Definition $avg
# Returns the average value of the numeric values that result from applying a specified expression to each document in a group of documents
# that share the same group by key.
# !!! $avg ignores non-numeric values. !!! => do not have to exclude documents with price == null

avg_pipeline = [
    { "$match" : {
        "varietyTonnage" : { "$exists": True }
        }
    },

    ### TODO: remove (just for testing)
    #{ "$limit" : 100 },

    { "$sort" : SON([("market", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            #"new_id" : bson.objectid.ObjectId,
            "date" : "$date",
            "market" : "$market",
            "commodity" : "$commodityTranslated"
        },
        "price" : {
            "$avg" : "$modalPrice"
        },
        "commodityTonnage" : { "$first": "$commodityTonnage"},
        "state" : { "$first": "$state"},
        "district" : { "$first": "$district"},
        "taluk" : { "$first": "$taluk"},
        "category" : { "$first": "$category"},
        ### varieties from which this entry has been derived
        "varieties" : { "$addToSet" : "$variety" }
        }
    },
    { "$project": {
        "_id" : 0, #"$_id.new_id",
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "market" : "$_id.market",
        "commodity" : "$_id.commodity",
        "price" : 1,
        "commodityTonnage" : 1,
        "state" : 1,
        "district" : 1,
        "taluk" : 1,
        "category" : 1,
        ### varieties from which this entry has been derived
        "varieties" : 1
        }
    }
]

### TODO: here price is computed in $project step from numerator and denominator
# NOTE: multiply may not work for null values
weighted_avg_pipeline = [
    { "$match" : {
        "varietyTonnage" : { "$exists": True }
        }
    },

    { "$limit" : 100 },

    { "$sort" : SON([("market", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            #"new_id" : bson.objectid.ObjectId,
            "date" : "$date",
            "market" : "$market",
            "commodity" : "$commodity"
        },
        "numerator" : { "$sum" : { "$multiply" : [ "$modalPrice", "$varietyTonnage" ] } },
        # keep denominator because variety tonnages not exact
        #"denominator" : { "$sum" : "$varietyTonnage" },

        "commodityTonnage" : { "$first" : "$commodityTonnage"},
        "state" : { "$first" : "$state"},
        "district" : { "$first" : "$district"},
        "taluk" : { "$first" : "$taluk"},
        "category" : { "$first" : "$category"},
        ### varieties from which this entry has been derived
        "varieties" : { "$addToSet" : "$variety" }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "market" : "$_id.market",
        "commodity" : "$_id.commodity",
        "price" : { "$divide" : [ "$numerator", "$commodityTonnage" ]},
        "commodityTonnage" : 1,
        "state" : 1,
        "district" : 1,
        "taluk" : 1,
        "category" : 1,
        ### varieties from which this entry has been derived
        "varieties" : 1
        }
    }
]

state_aggr_pipeline = [
    ### TODO: do a match here first if multiply does not work with null values
    { "$match" : {
        "commodityTonnage" : { "$exists": True, "$ne": np.nan }#,
        #"price" : {"$ne" : np.nan }
        }
    },
    #{ "$limit" : 10000 },

    ### NOTE: if several commodities in one collection also sort by commodity
    { "$sort" : SON([("state", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "state" : "$state",
            "commodity" : "$commodity"
        },
        "category" : { "$first" : "$category"},
        "numerator" : { "$sum" : { "$multiply" : [ "$price", "$commodityTonnage" ] } },
        "commodityTonnage" : { "$sum" : "$commodityTonnage" },
        "category" : { "$first" : "$category"},
        "districts" : { "$addToSet": "$district" },
        "markets" : { "$addToSet" : "$market" }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "state" : "$_id.state",
        "commodity" : "$_id.commodity",
        "category" : 1,
        "price" : { "$divide" : ["$numerator", "$commodityTonnage"]},
        "commodityTonnage" : 1,
        "districts" : 1,
        "markets": 1
        }
    }
]

district_aggr_pipeline = [
    ### TODO: do a match here first if multiply does not work with null values
    { "$limit" : 100 },

    { "$sort" : SON([("district", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "state" : "$state",
            "district" : "$district",
            "commodity" : "$commodity"
        },
        "category" : { "$first" : "$category"},
        "numerator" : { "$sum" : { "$multiply" : [ "$price", "$commodityTonnage" ] } },
        "commodityTonnage" : { "$sum" : "$commodityTonnage" },
        "category" : { "$first" : "$category"},
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "state": "$_id.state",
        "district" : "$_id.district",
        "commodity" : "$_id.commodity",
        "category" : 1,
        "price" : { "$divide" : ["$numerator", "$commodityTonnage"]},
        "commodityTonnage" : 1,
        }
    }
]

### Definition $out
# Takes the documents returned by the aggregation pipeline and writes them to a specified collection.
# The $out operator must be the last stage in the pipeline. The $out operator lets the aggregation framework return result sets of any size.
# => thanks to the $out operator we can avoid extracting (cursor + memory) and reinserting the data with pymongo.insert_many or odo

def add_out_coll(pipeline, out_coll):
    return pipeline + [{"$out" : out_coll}]

### import database handler
def comm_to_coll(comm, suffix=None, prefix=None):
    coll = '_'.join(comm.split())
    if suffix:
        coll = '_'.join([coll, suffix])
    if prefix:
        coll = '_'.join([prefix, coll])
    return coll.lower()

### TODO: do not need to aggregate by commodity, since files
### TODO: to explore aggregation details, pass explain=True

def aggregate_by_variety(db, cat_comm, weighted=False):
    print('weighted', weighted)
    for cat, comm_list in cat_comm.items():
        #if cat != 'Cereals':
        #    continue
        for comm in comm_list:
            coll = comm_to_coll(comm, 'varieties', 'market')
            out_coll = comm_to_coll(comm, None, 'market')
            print(coll, out_coll)
            ### TODO: different collections for (weighted) averages? otherwise the aggregation will overwrite one another
            if weighted:
                w_avg = add_out_coll(weighted_avg_pipeline, out_coll)
                print(w_avg)
                db.db[coll].aggregate(w_avg, allowDiskUse=True)
            else:
                avg = add_out_coll(avg_pipeline, out_coll)
                print(avg)
                db.db[coll].aggregate(avg, allowDiskUse=True)
    return

def aggregate_by_level(db, cat_comm, level):
    for cat, comm_list in cat_comm.items():
        #if cat != 'Cereals':
        #    continue
        for comm in comm_list:
            coll = comm_to_coll(comm, None, 'market')
            out_coll = comm_to_coll(comm, None, level)
            admin_wavg = []
            if level == 'state':
                admin_wavg = add_out_coll(state_aggr_pipeline, out_coll)
            else:
                admin_wavg = add_out_coll(district_aggr_pipeline, out_coll)
            print(admin_wavg)
            db.db[coll].aggregate(admin_wavg, allowDiskUse=True)
    return

def main():
    cat_comm = json.load(open(path.join(data_dir, 'commodities', 'selected_commodities.json'), 'r'))
    config = yaml.load(open('../db/db_config.yaml', 'r'))
    # for the purpose of testing, running with limit
    db = MongoDB(config['meteordb'], config['address'], config['meteorport'])
    #aggregate_by_variety(db, cat_comm)
    aggregate_by_level(db, cat_comm, 'state')
    return

if __name__ == "__main__":
    main()
