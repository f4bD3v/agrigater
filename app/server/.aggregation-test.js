db.state.paddy.aggregate(
{ $match: { date: { $gte : new Date(2015, 08, 01), $lt : new Date(2015, 09, 01) } } },
{ $group : {
    _id: { state: "$state" },
    na: { "$sum" : { "$cond" : [{"$gt" : ["$commodityTonnage", NaN] }, 0, 1]} } 
    }
})