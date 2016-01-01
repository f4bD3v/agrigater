/* 
	https://www.quora.com/Does-Meteor-risk-resetting-the-MongoDB-database-collections-on-app-restart
	``  if a collection with the requested name doesn't exist, 
		a new one with that name is made and then a client for using it is returned. 
		Otherwise, just a client for the existing collection is returned. ''
*/ 
function dayDiff(startDate, endDate) {
		//The 2nd argument to moment() is a parsing format rather than an display format.
		//that's why we want the .format() method:
		var a = moment(startDate);//.format('DD/MM/YYYY');
		console.log(a);
		var b = moment(endDate);//.format('DD/MM/YYYY');
		console.log(b);
		var days = b.diff(a, 'days');
		console.log(days);
		return days;
}

if (Meteor.isServer) {
	/* 
		Serving map layers through a publication: http://meteorcapture.com/publishing-data-from-an-external-api/
	*/
	Meteor.publish("getMapLayers", function() {
		// synchronous call to get file from public folder
  		var resp = HTTP.call('get', Meteor.absoluteUrl("/states.json"));
  		// add layer to collection 'mapLayers'
    	this.added("mapLayers", "states", resp.data); // Random.id()
  		//var resp = HTTP.call('get', Meteor.absoluteUrl("/districts.json"));
    	//this.added("mapLayers", "districts", resp.data);
  		var resp = HTTP.call('get', Meteor.absoluteUrl("/markets.geojson"));
    	this.added("mapLayers", "markets", JSON.parse(resp.content));
    	return this.ready();
  	});

  	Meteor.publish("getStateDistricts", function(stateName) {
		var districts = DistrictTopology.find({ state: stateName })  	
		if (districts) return districts;
		return this.ready();
  	});

  	Meteor.publish("getCommodities", function() {
  		// NOTE: findOne does not return a cursor, it returns a single document
    	var result = States.find({ _id : "India"}, { fields: { commodities: 1 } });
    	if (result)	return result;
    	return this.ready();
  	});

	/* TODO: differentiate between
		- variety price interface (one chart)
		- aggregated price with tonnage interface
	*/
	// what data to publish for time series interface component?: names of states/districts/(markets from admin unit => elasticSearch, not dropdown), names of commodities: what if a combination has no data?
	/* NOTE: DEPENDENT PUBLICATIONS ARE NOT REACTIVE (http://stackoverflow.com/questions/26398952/meteor-publish-publish-collection-which-depends-on-other-collection) 
	=> read about reactive joins: https://www.discovermeteor.com/blog/reactive-joins-in-meteor/
	*/

	/*Meteor.publish("commoditiesByAdminUnit", function(adminUnit, adminName) {
		var selector = { adminUnit : adminName };
		var options = { fields: { commodities: 1 } }
		var collName = titelize(adminUnit) + "s"
		data = global[collName].find(selector, options);
		return this.ready();
	});*/

	Meteor.publish("seriesMetaData", function(adminUnit, parentState, parentDistrict) {
		check(adminUnit, String);
		// TODO: aim is to hide lower-level data from user as long as she does not explicitely request it
		// parameter configurations:
		// - for a state: state, null, null
		// - for a district: district, "Rajasthan", null
		// - for a market: market, "Gholpur", "Rajasthan"
		// state: get all states

		// TODO: ADD COMMODITY VARIETIES => varieties field in commodities array objects

		if (adminUnit === 'state') {
			var data = [
				States.find({ commodities: {$ne: null} }, { fields : { name: 1, commodities: 1 } })
			];
		// district: get all districts in parent state and parent state
		} else if (adminUnit === 'district') {
			var data = [
				States.find({ _id : parentState, commodities: {$ne: null} }, { fields : { name: 1, commodities: 1 } }),
				Districts.find({ state : parentState, commodities: {$ne: null} }, { fields: { state: 1, name: 1, commodities: 1 } })
			];
		// market: get all markets in parent district as well as parent district and parent state
		} else if (adminUnit === 'market') {
			var data = [
				States.find({ _id : parentState, commodities: {$ne: null} }, { fields : { name: 1, commodities: 1 } }),
				Districts.find({ state: parentState, name: parentDistrict, commodities: {$ne: null} }, { fields: { state: 1, name: 1, commodities: 1 }}),
				Markets.find({ state: parentState, district: parentDistrict }, { fields: { state: 1, district: 1, name: 1, commodities: 1 }})
			];
		}
		/*
		data = [ 
			(adminUnit==='market') ? Markets.find({ parentUnit : parentUnitName, "market" : { $ne : adminName }}, { fields: { name : 1, commodities: 1 }}) : Markets.find({ adminUnit : adminName }, { fields: { name: 1, commodities: 1 } }),
			(adminUnit==='district') ? Districts.find({ state : parentUnitName}, { fields: { _id: 0, name: 1, commodities: 1 }}),
			(adminUnit==='state') ? States.find({}, { fields : { _id: 1, commodities: 1 }}) : States.find({ _id : }, { fields : { _id: 1, commodities: 1 }})
		];
		*/
		if (data) return data;
		return this.ready();
	});

	/* TODO: revise this according to defined components
		Generic data publication for time series chart, TODO: allow comparison => returning several series
		ADD FUNCTIONALITY TO DEAL WITH IMPUTATION
			imputed : { $exists : false } - get all entries that do not have an imputed field (=complete-imputed docs)
			vs
			complete : { $eq : true }, or
			imputed: { $eq : true } (get complete docs and docs completed through imputation)

		- how to use this.ready() in publications:
		https://forums.meteor.com/t/this-ready-in-a-publication/1681/9
	*/
	Meteor.publish("seriesChartData", function(adminUnit, unitName, commodity, comparisons) {
		check(adminUnit, String);
		check(unitName, String);
		check(commodity, String);

		var collName = titlelize(adminUnit)+commodity.replace(" ", "");
		// package for getting mongo collection instances by name (Mongo.Collection.get('foo') will return defined Foo = .. instance) : mongo-collection-instances
		var collection = Mongo.Collection.get(collName);
		var selector = {};
		selector[adminUnit] = unitName;
		// global chartDataOptions
		// NOTE: highcharts expects data to be sorted
		chartDataOptions = { 
		    sort: {
		        date: 1
		    },
		    fields : {
		        date: 1,
		        price: 1,
		        commodityTonnage: 1,
		        contrib: 1
		    }
		};
		chartDataOptions["fields"][adminUnit] = 1;

		var data = global[collName].find(selector, chartDataOptions);
		/*
		data.push(global[collName].find(selector, chartDataOptions).map(function(doc) { 
			docReplaceNan(doc)  // dataType
			// making use of anonymous function
		}));*/
		// comparison => data = [ find for parameters, find for combinationArray[0], .. find for combinationArray[n]]
		// for .. data.push
		if (data)
			return data;
		return this.ready();
	});

	// TODO: change this to getQuerySelector, the collection name is the same for queries that have to be combined
	getQuerySelector = function(adminUnit, unitName) {
		selector = {};
		selector[adminUnit] = unitName;
		// but shouldn't this be an $and to the $or? 
		// { $and: [ { state: stateName }, { $or: [ { district: shittybambam }, { district: stinkycowdung } ] } ] }
		return selector;
	}

	getCursor = function(adminUnit, unitName, commodity, state) {
		var collName = titlelize(adminUnit)+commodity.replace(" ", "");
		var selector = {};
		selector[adminUnit] = unitName;
		if (state !== null) {
			selector['state'] = state;
		}
		chartDataOptions = { 
		    sort: {
		        date: 1
		    }
		};
		/*
		    fields : {
		        date: 1,
		        price: 1,
		        commodityTonnage: 1,
		        contrib: 1
		    }

		var cursor = {
			adminUnit: adminUnit,
			name: unitName,
			commodity: commodity,
			id: [adminUnit, unitName, commodity].join('.'),
		*/
		cursor = global[collName].find(selector, chartDataOptions);
		//};
		return cursor;
	}

	Meteor.publish("initSeriesChartData", function(stateList, districtList, market, commodity) {
		sLength = stateList.length;
		dLength = districtList.length;
		var stateName = stateList[0];
		var districtName = districtList[0];
		var data = [];
		// TODO: assert sLength can't be 0
		if (sLength === 1) {
			var cursor = getCursor('state', stateName, commodity, null);
			data.push(cursor);
		} else {
			// sLength series (direct comparison)	
			// TODO: create an $or query so to publish only one cursor
			// { $or: [ { <expression1> }, { <expression2> }, ... , { <expressionN> } ] }
			// find ( or-expression )
			var collName = titlelize('state')+commodity.replace(' ', '');
			or = [];
			for (i in stateList) {
				var stateName = stateList[i];
				or.push(getQuerySelector('state', stateName));
				//var cursor = getCursor('state', stateName, commodity, null);
				//data.push(cursor);
			}
			var unionCursor = global[collName].find({ $or: or }, { sort: { date: 1 }});
			data.push(unionCursor);
		}
		// NOTE: dLength can be 0 ==> do nothing
		if (dLength === 1) {
			// one state, one district
			var cursor = getCursor('district', districtName, commodity, stateName);
			data.push(cursor);
		} else if (dLength > 1) {
			// one state, dLength districts (direct comparison)
			// TODO: create an $or query so as to publish only one cursor from the same collection
			var collName = titlelize('district')+commodity.replace(' ', '');
			or = [];	
			for (i in districtList) {
				var districtName = districtList[i];
				or.push(getQuerySelector('district', districtName));
				//var cursor = getCursor('district', districtName, commodity, stateName);
				//data.push(cursor);
			}
			// { $and: [ { state: stateName }, { $or: [ { district: shittybambam }, { district: stinkycowdung } ] } ] }
			var unionCursor = global[collName].find({ $and: [ { state: stateName }, { $or: or } ]}, { sort: { date: 1 }});
			data.push(unionCursor);
		}
		if (market !== null) {
			var cursor = getCursor('market', market, commodity, stateName);
			data.push(cursor);
			// one state series, one district series, one market series	
		}
		if (data)
			return data;
		return this.ready();
	});

	Meteor.publish("getStatesByCommodity", function(commodity) {
		check(commodity, String);
		var self = this;

		var data = []
		if (commodity !== 'all') {
			data = States.find({ 'commodities.commodity': commodity }, { fields: { state: 1, commodities: 1 } }).fetch();
			_(data).each(function(d) {
				//var commodityObj = d.commodities.filter(function(obj) { return obj.commodity === commodity; })[0];
				var commodityObj = _.findWhere(d.commodities, {commodity: commodity});
				d.commodity = commodityObj.commodity;
				d.recordsTotal = commodityObj.recordsTotal;
				d.recordsCoverage = commodityObj.recordsCoverage;
				delete d['commodities'];
				self.added("states", Random.id(), d);
			});
		}
		//if (data)
		//	return data;
		return this.ready();
	});

	Meteor.publish("getDistrictsByCommodity", function(commodity, state) {
		check(commodity, String);
		var self = this;

		var data = []
		if (commodity !== 'all') {
			data = Districts.find({ 'commodities.commodity': commodity, state: state }, { fields: { state: 1, district: 1, commodities: 1 } }).fetch();
			_(data).each(function(d) {
				//var commodityObj = d.commodities.filter(function(obj) { return obj.commodity === commodity; })[0];
				var commodityObj = _.findWhere(d.commodities, {commodity: commodity});
				d.commodity = commodityObj.commodity;
				d.recordsTotal = commodityObj.recordsTotal;
				d.recordsCoverage = commodityObj.recordsCoverage;
				delete d['commodities'];
				self.added("districts", Random.id(), d);
			});
		}
		//if (data)
		//	return data;
		return this.ready();
	});

	Meteor.publish("getMarketsByCommodity", function(commodity, state) {
		check(commodity, String);
		selector = {};
		selector['commodities.commodity'] = commodity;
		if (state !== 'India')
			selector.state = state;
		var data = [];
		// TODO: next commodities.records: {$gt : threshold }
		// fetch data, then add scope variable for each record?
		if (commodity === 'all')
			data = Markets.find({}, { fields: { state: 1, district: 1, market: 1, commodities: 1 } });
		else	
			data = Markets.find(selector, { fields: { state: 1, district: 1, market: 1, commodities: 1 } });
		if (data)
			return data;
		return this.ready();
	});

	Meteor.publish("getTonnageAggregates", function(adminUnit, commodity, rangeStart, rangeEnd, parentState) {
		var self = this;
		var id = {
			state: "$state"
		};
		if (adminUnit === 'district')
			id[adminUnit] = "$district";
		var match = {
			date: { $gte: rangeStart, $lt: rangeEnd },
			commodityTonnage: { $ne: NaN }
		};
		if (adminUnit === 'district')
			match['state'] = parentState;
		var project = {
			_id: 0,
			state: "$_id.state",
			aggrTonnage: 1,
			commodity: 1
		}
		if (adminUnit === 'district')
			project.district = "$_id.district";

		aggrOps = [
			{ $match: match }, 
     		//{ $sort : { state: 1, date: 1 } },
			{ $group: {
				_id: id,
				commodity: {"$first" : "$commodity"},
				aggrTonnage : {"$sum" : "$commodityTonnage"}
				}
			}, 
			{ $project: project }
		];
		// TODO: fix collName issue for commodity names with spaces
		var collName = titlelize(adminUnit)+commodity.replace(" ", "");
		console.log(collName);
		console.log(rangeStart);
		console.log(rangeEnd);
		var aggregates = global[collName].aggregate(aggrOps);
		// publish this to aggregateTonnage on the clientside
		_(aggregates).each(function(aggregate) {
			aggregate.startDate = rangeStart;
			aggregate.endDate = rangeEnd;
			if (adminUnit === 'district')
				aggregate.name = aggregate.district;
			else
				aggregate.name = aggregate.state;
			if (aggregate.name !== NaN)
				self.added("aggregateTonnage", Random.id(), aggregate);
		});
		return this.ready();
	});

	Meteor.publish("stateStatistics", function(commodity, thresholds, rangeStart, rangeEnd) {
		var self = this;
		var days = dayDiff(rangeStart, rangeEnd);
		console.log(days);

		// NOTE: market level collections ==> total nb. of records (unless group by date and "$sum": 1) 
		// DO NOT NEED TO AGGREGATE USING MARKET LEVEL COLLECTIONS DUE TO CONTRIB FIELD
		var stateAggrOps = [
			{ $match: {
				date: { $gte: rangeStart, $lt: rangeEnd },
				}
			},
     		{ $sort : { state: 1 } },
			{ $group: {
				_id: {
					state: "$state"
				},
				commodity: {"$first" : "$commodity"},
				recordsCoverage: { "$sum" : 1 },
				recordsTotal: { "$sum" : "$contrib" },
				missingTonnagesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$commodityTonnage", NaN] }, 0, 1]} },
				missingPricesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$price", NaN] }, 0, 1]} },
				}
			},
			{ $project: {
				state: "$_id.state",
				commodity: 1,
				missingTonnagesTotal: 1,
				missingTonnages: { "$multiply" : [100, { "$divide" : [ "$missingTonnagesTotal", "$recordsCoverage" ]}] },
				missingPricesTotal: 1,	
				missingPrices: { "$multiply" : [100, { "$divide" : [ "$missingPricesTotal", "$recordsCoverage" ]}] },
				recordsCoverage: 1,
				recordsTotal: 1,
				coverage: { "$multiply" : [100, { "$divide" : ["$recordsCoverage", days]}] }
				}
			}
		];
		var collName = "State"+commodity.replace(" ", "");
		console.log(collName);
		// TODO: find out how to expose allowDiskUse option to meteor aggregate package: https://github.com/meteorhacks/meteor-aggregate/issues/11
		var aggregates = global[collName].aggregate(stateAggrOps)//, allowDiskUse=true);
		_(aggregates).each(function(aggregate) {
			aggregate.coverage = Math.round(aggregate.coverage);
			aggregate.missingPrices = Math.round(aggregate.missingPrices);
			aggregate.missingTonnages = Math.round(aggregate.missingTonnages);
			// NOTE: filtering on threshold before adding to collection
			// missingPrices < thresholds.priceNA && missingTonanges < threshold.tonnageNA && coverage > threshold.coverageNA
			if (aggregate.missingPrices < thresholds.missingPrices && aggregate.missingTonnages < thresholds.missingTonnages && aggregate.coverage > thresholds.coverage) {
				aggregate.startDate = rangeStart;
				aggregate.endDate = rangeEnd;
				self.added("stateStatistics", Random.id(), aggregate);
			}
		});
		return this.ready();
	});

	Meteor.publish("districtStatistics", function(stateName, commodity, thresholds, rangeStart, rangeEnd) {
		var self = this;
		// TODO: find out if moment can convert from Date object
		console.log(rangeStart);
		console.log(rangeEnd);
		var days = dayDiff(rangeStart, rangeEnd);

		// NOTE: market level collections ==> total nb. of records (unless group by date and "$sum": 1) 
		// DO NOT NEED TO AGGREGATE USING MARKET LEVEL COLLECTIONS DUE TO CONTRIB FIELD
		var districtAggrOps = [
			{ $match: {
				// simple equality match with string:
				state: stateName,
				date: { $gte: rangeStart, $lt: rangeEnd }
				}	
			},
			// TODO: create district collections per state!	
     		//{ $sort : { state: 1, district: 1 } },
			{ $group: {
				_id: {
					district: "$district"
				},
				state: {"$first" : "$state"}, // already matched on state
				commodity: {"$first" : "$commodity"},
				recordsCoverage: { "$sum" : 1 },
				recordsTotal: { "$sum" : "$contrib" },
				missingTonnagesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$commodityTonnage", NaN] }, 0, 1]} },
				missingPricesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$price", NaN] }, 0, 1]} },
				}
			},
			{ $project: {
				district: "$_id.district",
				state: 1,
				commodity: 1,
				missingTonnagesTotal: 1,
				missingTonnages: { "$multiply" : [100, { "$divide" : [ "$missingTonnagesTotal", "$recordsCoverage" ]}] },
				missingPricesTotal: 1,	
				missingPrices: { "$multiply" : [100, { "$divide" : [ "$missingPricesTotal", "$recordsCoverage" ]}] },
				recordsCoverage: 1,
				recordsTotal: 1,
				coverage: { "$multiply" : [100, { "$divide" : ["$recordsCoverage", days]}] }
				}
			}
		];
		var collName = "District"+commodity.replace(" ", "");
		var aggregates = global[collName].aggregate(districtAggrOps);
		_(aggregates).each(function(aggregate) {
			aggregate.coverage = Math.round(aggregate.coverage);
			aggregate.missingPrices = Math.round(aggregate.missingPrices);
			aggregate.missingTonnages = Math.round(aggregate.missingTonnages);
			// NOTE: filtering on threshold before adding to collection
			// missingPrices < thresholds.priceNA && missingTonanges < threshold.tonnageNA && coverage > threshold.coverageNA
			if (aggregate.missingPrices < thresholds.missingPrices && aggregate.missingTonnages < thresholds.missingTonnages && aggregate.coverage > thresholds.coverage) {
				aggregate.startDate = rangeStart;
				aggregate.endDate = rangeEnd;
				self.added("districtStatistics", Random.id(), aggregate);
			}
		});
		return this.ready();
	});

	Meteor.publish("marketStatistics", function(stateName, commodity, thresholds, rangeStart, rangeEnd) {
		var self = this;
		// TODO: find out if moment can convert from Date object
		console.log(stateName);
		console.log(rangeStart);
		console.log(rangeEnd);
		var days = dayDiff(rangeStart, rangeEnd);

		var match = {
			$match: {
				date: { $gte: rangeStart, $lt: rangeEnd }
			}	
		};
		if (stateName !== 'India') {
			match["$match"].state = stateName;
		}
		console.log(match);

		var marketAggrOps = [
			match,
     		//{ $sort : { market: 1 } },
			{ $group: {
				_id: {
					market: "$market",
					district: "$district"
				},
				commodity: {"$first" : "$commodity"},
				state: {"$first" : "$state"},
				recordsCoverage: { "$sum" : 1 },
				missingTonnagesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$commodityTonnage", NaN] }, 0, 1]} },
				missingPricesTotal: { "$sum" : { "$cond" : [{"$gt" : ["$price", NaN] }, 0, 1]} },
				}
			},
			{ $project: {
				market: "$_id.market",
				district: "$_id.district",
				state: 1,
				commodity: 1,
				missingTonnagesTotal: 1,
				missingTonnages: { "$multiply" : [100, { "$divide" : [ "$missingTonnagesTotal", "$recordsCoverage" ]}] },
				missingPricesTotal: 1,	
				missingPrices: { "$multiply" : [100, { "$divide" : [ "$missingPricesTotal", "$recordsCoverage" ]}] },
				recordsCoverage: 1,
				coverage: { "$multiply" : [100, { "$divide" : ["$recordsCoverage", days]}] }
				}
			}
		];

		console.log(thresholds.missingTonnages);
		console.log(thresholds.missingPrices);

		var collName = "Market"+commodity.replace(" ", "");
		var aggregates = global[collName].aggregate(marketAggrOps) //, allowDiskUse=true);
		_(aggregates).each(function(aggregate) {
			aggregate.coverage = Math.round(aggregate.coverage);
			aggregate.missingPrices = Math.round(aggregate.missingPrices);
			aggregate.missingTonnages = Math.round(aggregate.missingTonnages);
			// NOTE: filtering on threshold before adding to collection
			// missingPrices < thresholds.priceNA && missingTonanges < threshold.tonnageNA && coverage > threshold.coverageNA
			if (aggregate.missingPrices < thresholds.missingPrices && aggregate.missingTonnages < thresholds.missingTonnages && aggregate.coverage > thresholds.coverage) {
				aggregate.startDate = rangeStart;
				aggregate.endDate = rangeEnd;
				if (!(typeof aggregate.district === 'string' || aggregate.district instanceof String)) aggregate.district = '';
				self.added("marketStatistics", Random.id(), aggregate);
			}
		});
		return this.ready();
	});

	Meteor.publish("states", function() {
		var data = States.find({ _id : { $ne : "India"}});
		if (data)
			return data;
		return this.ready();
	});

	/*
	Meteor.publish("india", function() {
		return States.find({ _id : "India"});
	});*/


}

// IMPORTANT: calling collections with references and names: https://dweldon.silvrback.com/collections-by-reference