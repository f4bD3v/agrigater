function getCollectionName(adminUnit, commodity, varieties) {
    collectionName = adminUnit + "." + commodity.toLowerCase().replace("-", "").replace(" ", "");
    if (varieties)
     collectionName = collectionName + "." + varieties;
    return collectionName;
}

titlelize = function(str) {
    return str.substr(0, 1).toUpperCase() + str.substr(1);
}

collTitleCase = function(str) {
    parts = str.split(".");
    var toJoin = _.map(parts, titlelize);//function(val) { return titlelize(val); }
    return toJoin.join('');
}

function instantiateCommodityCollections(commodityObj) {
    var adminUnits = ["state", "district", "market", "india"];
    for (key in commodityObj) {
        commodityArr = commodityObj[key];
        for (i in commodityArr) {
            for (j in adminUnits) {
                collectionName = getCollectionName(adminUnits[j], commodityArr[i], false);
                if (Meteor.isServer) {
                    global[collTitleCase(collectionName)] = new Mongo.Collection(collectionName);
                }
                if (Meteor.isClient) {
                    //console.log(collectionName);
                    window[collTitleCase(collectionName)] = new Mongo.Collection(collectionName);
                }

                collectionName = getCollectionName(adminUnits[j], commodityArr[i], 'varieties');
                if (Meteor.isServer) {
                    global[collTitleCase(collectionName)] = new Mongo.Collection(collectionName);
                }
                if (Meteor.isClient) {
                    //console.log(collectionName);
                    window[collTitleCase(collectionName)] = new Mongo.Collection(collectionName);
                }
            }
        }
    }
}

// db collections
States = new Mongo.Collection("states");
Districts = new Mongo.Collection("districts");
Markets = new Mongo.Collection("markets");
DistrictTopology = new Mongo.Collection("state.district.topologies");

// minimongo collections created for application:
MapLayers = new Mongo.Collection("mapLayers");
AggregateTonnage = new Mongo.Collection("aggregateTonnage");
StateStatistics = new Mongo.Collection("stateStatistics");
DistrictStatistics = new Mongo.Collection("districtStatistics");
MarketStatistics = new Mongo.Collection("marketStatistics");

// might this save me ?https://github.com/peerlibrary/meteor-server-autorun => not necessary
if (Meteor.isServer) {
    var res = States.findOne({ _id : "India"}, { fields: { commodities: 1 } });
    console.log(res);
    if (res) {
        instantiateCommodityCollections(res["commodities"]);
    }
}

if (Meteor.isClient) {
    Tracker.autorun(function(c) {
        commoditiesHandle = Meteor.subscribe('getCommodities');
        if (commoditiesHandle.ready()) {
           var res = States.findOne({ _id : "India"}, { fields: { commodities: 1 } });
           if (res) {
               instantiateCommodityCollections(res["commodities"]);
               c.stop();
           }
        }
    });
}


//States = new Mongo.Collection("states");
//Districts = new Mongo.Collection("districts");
//Markets = new Mongo.Collection("markets");
