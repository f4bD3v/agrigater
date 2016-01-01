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

india_aggr_pipeline = [
    { "$match" : {
        "commodityTonnage" : { "$exists": True, "$ne": 0, "$ne": np.nan, "$ne": None }
        }
    },
    { "$sort" : SON([("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "commodity" : "$commodity"
        },
        "category" : { "$first" : "$category"},
        "numerator" : { "$sum" : { "$multiply" : [ "$price", "$commodityTonnage" ] } },
        "commodityTonnage" : { "$sum" : "$commodityTonnage" },
        "category" : { "$first" : "$category"},
        "states" : { "$addToSet": "$state" },
        "districts" : { "$addToSet": "$district" },
        "markets" : { "$addToSet" : "$market" },
        "contrib" : { "$sum": 1 }
        }
    },
     { "$project" : {
        "_id": 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date": "$_id.date",
        "commodity": "$_id.commodity",
        "category": 1,
        "price": { "$divide" : ["$numerator", "$commodityTonnage"]},
        "commodityTonnage" : 1,
        "states": 1,
        "districts": 1,
        "markets": 1,
        "contrib": 1
        }
    }
]
### TODO: what about recording contributing varieties at state level?

# adding variety fields: http://stackoverflow.com/questions/26904783/how-to-merge-array-field-in-document-in-mongo-aggregation
state_aggr_pipeline = [
    ### TODO: do a match here first if multiply does not work with null values
    { "$match" : {
        ### NOTE: "$ne": np.nan works to exclude NaN
        "commodityTonnage" : { "$exists": True, "$ne": 0, "$ne": np.nan, "$ne": None }
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
        "markets" : { "$addToSet" : "$market" },
        "contrib" : { "$sum": 1}
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
        "markets": 1,
        "contrib": 1
        }
    }
]

district_aggr_pipeline = [
    ### TODO: do a match here first if multiply does not work with null values
    { "$match" : {
        ### NOTE: "$ne": np.nan works to exclude NaN
        "commodityTonnage" : { "$exists": True, "$ne": 0, "$ne": np.nan, "$ne": None }#,
        #"price" : {"$ne" : np.nan }
        }
    },
    #{ "$limit" : 100 },
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
        "markets" : { "$addToSet" : "$market" },
        "contrib" : { "$sum": 1 }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "state": "$_id.state",
        "district" : "$_id.district",
        "commodity" : "$_id.commodity",
        "price" : { "$divide" : ["$numerator", "$commodityTonnage"]},
        "commodityTonnage" : 1,
        "category" : 1,
        "markets" : 1,
        "contrib" : 1
        }
    }
]

india_variety_aggr = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$sort" : SON([("district", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "commodity": "$commodity",
            "variety" : "$variety"
        },
        "category" : { "$first" : "$category"},
        "price" : { "$avg" : "$price"},
        "contrib" : { "$sum": 1 }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "category" : 1,
        "commodity" : "$_id.commodity",
        "price" : 1,
        "contrib" : 1
        }
    }
]

state_variety_aggr = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$sort" : SON([("district", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "state" : "$state",
            "commodity": "$commodity",
            "variety" : "$variety"
        },
        "category" : { "$first" : "$category"},
        "price" : { "$avg" : "$price"},
        "contrib" : { "$sum": 1 }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "state" : "$_id.state",
        "category" : 1,
        "commodity" : "$_id.commodity",
        "price" : 1,
        "contrib" : 1
        }
    }
]

district_variety_aggr = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$sort" : SON([("district", 1), ("date", 1)]) },
    { "$group" : {
        "_id" : {
            "date" : "$date",
            "state" : "$state",
            "district" : "$district",
            "commodity": "$commodity",
            "variety" : "$variety"
        },
        "category" : { "$first" : "$category"},
        "price" : { "$avg" : "$price"},
        "contrib" : { "$sum": 1 }
        }
    },
    { "$project" : {
        "_id" : 0, # try this: otherwise: 0
        ## deconstruct the _id field => new _id will be generated upon reinserting
        "date" : "$_id.date",
        "district" : "$_id.district",
        "state" : "$_id.state",
        "category" : 1,
        "commodity" : "$_id.commodity",
        "price" : 1,
        "contrib" : 1
        }
    }
]

state_records = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$group" : {
        "_id" : {
            "state" : "$state",
        },
        "category" : { "$first" : "$category" },
        "commodity" : { "$first" : "$commodity" },
        "recordsCoverage" : { "$sum" : 1 },
        "recordsTotal" : { "$sum" : "$contrib" }
        }
    },
    { "$project" : {
        "_id" : 0,
        "state" : "$_id.state",
        "category": 1,
        "commodity": 1,
        "recordsCoverage": 1,
        "recordsTotal": 1
        }
    }
];

### TODO: remove Cereals specificity
district_records = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$group" : {
        "_id" : {
            "state" : "$state",
            "district" : "$district"
        },
        "category" : { "$first" : "$category" },
        "commodity" : { "$first" : "$commodity" },
        "recordsCoverage" : { "$sum" : 1 },
        "recordsTotal" : { "$sum" : "$contrib" }
        }
    },
    { "$project" : {
        "_id" : 0,
        "state" : "$_id.state",
        "district" : "$_id.district",
        "category": 1,
        "commodity": 1,
        "recordsCoverage": 1,
        "recordsTotal": 1
        }
    }
];

district_distinct = [
    { "$group" : {
        "_id" : {
            "state" : "$state",
            "district" : "$district"
            }
        }
    },
    { "$project"  : {           
        "_id" : 0,
        "state": "$_id.state",
        "district" : "$_id.district",
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
    coll = '.'.join(comm.split())
    if suffix:
        coll = '.'.join([coll, suffix])
    if prefix:
        coll = '.'.join([prefix, coll])
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
            pipeline = weighted_avg_pipeline if weighted else avg_pipeline
            pipeline = add_out_coll(pipeline, out_coll)
            print(pipeline)
            db.db[coll].aggregate(pipeline, allowDiskUse=True)
            """
            if weighted:
                w_avg = add_out_coll(weighted_avg_pipeline, out_coll)
                print(w_avg)
                db.db[coll].aggregate(w_avg, allowDiskUse=True)
            else:
                avg = add_out_coll(avg_pipeline, out_coll)
                print(avg)
                db.db[coll].aggregate(avg, allowDiskUse=True)
            """
    return

def update_commodity_fields(db, comm_by_unit, level):
    for state in list(comm_by_unit.keys()): 
        if level == 'state':
            db.db['states'].update_one({"_id" : state}, { "$set" : { "commodities" : comm_by_unit[state] } })
        elif level == 'district':
            for district in list(comm_by_unit[state].keys()):
                db.db['districts'].update_one({"state" : state}, { "$set" : { "commodities" : comm_by_unit[state][district] } })
        elif level == 'market':
            for district in list(comm_by_unit[state].keys()):
                for market in list(comm_by_unit[state][district].keys()):
                    db.db['markets'].update_one({"market": market, "district": district, "state": state}, { "$set" : {"commodities" : comm_by_unit[state][district][market] } }, upsert=True)
    return

### TODO: how get all varieties in a state, all in a district
def create_admin_commodity_dict(items, cat, comm, comm_by_unit):
    #comm_by_unit = dict(comm_by_unit)
    ### TODO: implement adding varieties to unit obj 
    print(items)
    for item in items:
        if 'district' not in item:
            #state=item
            state = item.pop('state', None)
            if state not in comm_by_unit:
                print(comm_by_unit)
                comm_by_unit[state] = [item]
            else:
                comm_by_unit[state] = comm_by_unit[state] + [item]
            """
            if state not in comm_by_unit:
                print('resetting')
                print(state)
                print(comm_item)
                comm_by_unit[state] = None
                comm_by_unit[state] = [comm_item]
                #comm_by_unit.update({state: [comm_item]})
            else:
                print('adding')
                print(comm_item)
                print(state)
                comm_by_unit[state] = comm_by_unit[state] + [comm_item]
                print(comm_by_unit[state])
            print(comm_by_unit)
            """
        else:
            ### where to add varieties? => works out of the box for markets
            district = item.pop('district', None)
            state = item.pop('state', None)
            if not state in comm_by_unit:
                comm_by_unit[state] = {}
            if 'market' in item:
                market = item.pop('market', None)
                if not district in comm_by_unit[state]:
                    comm_by_unit[state][district] = {}
                if not market in comm_by_unit[state][district]:
                    comm_by_unit[state][district][market] = [item]
                else:    
                    comm_by_unit[state][district][market] = comm_by_unit[state][district][market] + [item]
            else:
                if not district in comm_by_unit[state]:
                    comm_by_unit[state][district] = [item]
                else:
                    comm_by_unit[state][district] = comm_by_unit[state][district] + [item]
    return

def aggregate_by_level(db, cat_comm, level):
    # level = None
    for cat, comm_list in cat_comm.items():
        if cat != 'Cereals':
            continue
        for comm in comm_list:
            coll = comm_to_coll(comm, None, 'market')
            out_coll = comm_to_coll(comm, None, level)
            pipeline = india_aggr_pipeline
            if level == 'state':
                pipeline = state_aggr_pipeline
            elif level == 'district':
                pipeline = district_aggr_pipeline
            pipeline = add_out_coll(pipeline, out_coll)
            print(pipeline)
            db.db[coll].aggregate(pipeline, allowDiskUse=True)
    return

def aggregate_varieties_by_level(db, cat_comm, level):
    for cat, comm_list in cat_comm.items():
        if cat != 'Cereals':
            continue
        for comm in comm_list:
            coll = comm_to_coll(comm, 'varieties', 'market')
            out_coll = comm_to_coll(comm, 'varieties', level)
            pipeline = india_aggr_pipeline
            if level == 'state':
                pipeline = state_aggr_pipeline
            elif level == 'district':
                pipeline = district_aggr_pipeline
            pipeline = add_out_coll(pipeline, out_coll)
            print(pipeline)
            db.db[coll].aggregate(pipeline, allowDiskUse=True)
    return


def merge_objs(unit_objs, variety_objs, district):
    for unit_obj in unit_objs:
        for variety_obj in variety_objs:
            if unit_obj["state"] != variety_obj["state"]:
                continue
            if district:
                if unit_obj["district"] != variety_obj["district"]:
                    continue
            unit_obj["varieties"] = variety_obj["varieties"]

### NOTE: computing total records for state/districts at market level, not series coverage at state level
def insert_commodities_by_level(db, cat_comm, level):
    comm_by_unit = {}
    for cat, comm_list in cat_comm.items():
        if cat != 'Cereals':
            continue
        for comm in comm_list:
            ### gather info and then do an insert
            coll = comm_to_coll(comm, None, 'market')
            out_coll = comm_to_coll(comm, None, level)
            if level == 'state':
                #state_list = db.db[coll].distinct('state')
                ## count records for every state in state-level collection
                state_objs = list(db.db[out_coll].aggregate(state_records))
                variety_objs = list(db.db[coll].aggregate(state_varieties, allowDiskUse=True))
                merge_objs(state_objs, variety_objs, False)
                print(state_objs)
                """
                for state in state_list:
                    if not state in comm_by_unit:
                        comm_by_unit[state] = {}
                    if cat in comm_by_unit[state]:
                        comm_by_unit[state][cat] = comm_by_unit[state][cat] + [comm]
                    else:
                        comm_by_unit[state][cat] = [comm]
                """
                #comm_by_unit = create_admin_commodity_dict(state_list, cat, comm, comm_by_unit)
                create_admin_commodity_dict(state_objs, cat, comm, comm_by_unit)
            else:
                #objs = list(db.db[coll].aggregate(district_distinct))
                ### TODO: figure out which collection to use
                dist_objs = list(db.db[out_coll].aggregate(district_records))
                variety_objs = list(db.db[coll].aggregate(district_varieties, allowDiskUse=True))
                merge_objs(dist_objs, variety_objs, True)
                print(variety_objs  )
                create_admin_commodity_dict(dist_objs, cat, comm, comm_by_unit)
                """
                for obj in objs:
                    state = obj['state']
                    district = obj['district']
                    if not state in comm_by_unit:
                        comm_by_unit[state] = {}
                    if not district in comm_by_unit[state]:
                        comm_by_unit[state][district] = {}
                    if cat in comm_by_unit[state][district]:
                        comm_by_unit[state][district][cat] = comm_by_unit[state][district][cat] + [comm]
                    else:
                        comm_by_unit[state][district][cat] = [comm]
                """
                # { "state" : .., "district": .. }
        ### NOTE:        
        # after performing aggregation states and districts docs' commodities field needs to be updated with all the commodities present in their territorry
    ### NOTE: works on the basis that aggregated collections are specific to commodities and therefore only contain districts or states that trade in said commodity
    for state, item_list in comm_by_unit.items():
        print(state)
        for item in item_list:
            print(item)
        print()
    #print(comm_by_unit)
    update_commodity_fields(db, comm_by_unit, level)
    return 

### NOTE: for markets records directly translate to coverage 
commodity_markets = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$group" : {
        "_id" : {
            "market": "$market",
            "state": "$state",
            "district": "$district"
        },
        "category" : { "$first" : "$category" },
        "commodity" : { "$first" : "$commodity" },
        "varieties": { "$addToSet": "$variety"},
        "records" : { "$sum" :1 }
        }
    },
    { "$project" : {
        "_id" : 0,
        "market" : "$_id.market",
        "district" : "$_id.district",
        "state" : "$_id.state",
        "category": 1,
        "commodity": 1,
        "varieties": 1,
        "records": 1
        }
    }
]

market_commodities = [
    { "$match" : {
        "category" : "Cereals"
        }
    },
    { "$group" : {
        "_id" : {
            "market": "$market",
            "state": "$state",
            "district": "$district"
        },
        "category" : { "$first" : "$category" },
        "commodity" : { "$first" : "$commodity" },
        "vvarieties": { "$push": "$varieties"},
        "records" : { "$sum" :1 }
        }
    },
    { "$unwind": "$vvarieties"},
    { "$unwind": "$vvarieties"},
    { "$group": {
        "_id": "$_id",
        "category": { "$first": "$category"},
        "commodity" : { "$first" : "$commodity" },
        "records" : { "$first" : "$records" },
        "varieties": { "$addToSet": "$vvarieties"}
    }},
    { "$project" : {
        "_id" : 0,
        "market" : "$_id.market",
        "district" : "$_id.district",
        "state" : "$_id.state",
        "category": 1,
        "commodity": 1,
        "varieties": 1,
        "records": 1
        }
    }
]

market_varieties = [
    { "$match": {
        "category": "Cereals"
        }
    },
    { "$group": {
        "_id": {
            "market": "$market",
            "state": "$state",
            "district": "$district",
            "commodity": "$commodity"
        },
        "category": { "$first": "$category"},
        "vvarieties": { "$push": "$varieties"}
        }
    },
    { "$unwind": "$vvarieties"},
    { "$unwind": "$vvarieties"},
    { "$group": {
        "_id": "$_id",
        "category": { "$first": "$category"},
        "varieties": { "$addToSet": "$vvarieties"}
    }},
    { "$project": {
        "_id": 0,
        "market" : "$_id.market",
        "district" : "$_id.district",
        "state" : "$_id.state",
        "commodity" : "$_id.commodity",
        "category": 1,
        "varieties": 1
        }
    }
]

# on market collection
district_varieties = [
    { "$match": {
        "category": "Cereals"
        }
    },
    { "$group": {
        "_id": {
            "state": "$state",
            "district": "$district",
            "commodity": "$commodity"
        },
        "category": { "$first": "$category"},
        "vvarieties": { "$push": "$varieties"}
        }
    },
    { "$unwind": "$vvarieties"},
    { "$unwind": "$vvarieties"},
    { "$group": {
        "_id": "$_id",
        "category": { "$first": "$category"},
        "varieties": { "$addToSet": "$vvarieties"}
    }},
    { "$project": {
        "_id": 0,
        "district" : "$_id.district",
        "state" : "$_id.state",
        "commodity" : "$_id.commodity",
        "category": 1,
        "varieties": 1
        }
    }
]

state_varieties = [
    { "$match": {
        "category": "Cereals"
        }
    },
    { "$group": {
        "_id": {
            "state": "$state",
            "commodity": "$commodity"
        },
        "category": { "$first": "$category"},
        "vvarieties": { "$push": "$varieties"}
        }
    },
    { "$unwind": "$vvarieties"},
    { "$unwind": "$vvarieties"},
    { "$group": {
        "_id": "$_id",
        "category": { "$first": "$category"},
        "varieties": { "$addToSet": "$vvarieties"}
    }},
    { "$project": {
        "_id": 0,
        "state" : "$_id.state",
        "commodity" : "$_id.commodity",
        "category": 1,
        "varieties": 1
        }
    }
]

def insert_market_commodities(db, cat_comm):
    comm_by_unit = {}
    for cat, comm_list in cat_comm.items():
        if cat != 'Cereals':
            continue
        for comm in comm_list:
            coll = comm_to_coll(comm, None, 'market')
            print(coll)
            markets = db.db[coll].aggregate(market_commodities, allowDiskUse=True)
            create_admin_commodity_dict(markets, cat, comm, comm_by_unit)
    print(comm_by_unit)
    update_commodity_fields(db, comm_by_unit, 'market')
    return

def main():
    cat_comm = json.load(open(path.join(data_dir, 'commodities', 'selected_commodities.json'), 'r'))
    config = yaml.load(open('../db/db_config.yaml', 'r'))
    # for the purpose of testing, running with limit
    db = MongoDB(config['meteordb'], config['address'], config['meteorport'])
    #aggregate_by_variety(db, cat_comm)
    #insert_market_commodities(db, cat_comm)
    #insert_commodities_by_level(db, cat_comm, 'state')
    aggregate_by_level(db, cat_comm, 'india')
    #aggregate_by_level(db, cat_comm, 'state')
    #aggregate_by_level(db, cat_comm, 'district')
    #aggregate_varieties_by_level(db, cat_comm, 'india')
    #aggregate_varieties_by_level(db, cat_comm, 'state')
    #aggregate_varieties_by_level(db, cat_comm, 'district')
    #insert_commodities_by_level(db, cat_comm, 'district')
    return

if __name__ == "__main__":
    main()
