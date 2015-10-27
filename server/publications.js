/* 
	https://www.quora.com/Does-Meteor-risk-resetting-the-MongoDB-database-collections-on-app-restart
	``  if a collection with the requested name doesn't exist, 
		a new one with that name is made and then a client for using it is returned. 
		Otherwise, just a client for the existing collection is returned. ''
*/ 
if (Meteor.isServer) {
	// On server -> sets up collection
	States = new Mongo.Collection("states");
	Districts = new Mongo.Collection("districts");
	Markets = new Mongo.Collection("markets");

	Meteor.publish('india', function() {
		return States.find({ _id : 'India'});
	});
	Meteor.publish('states', function() {
		return States.find({ _id : { $ne : 'India'}});
	});
}