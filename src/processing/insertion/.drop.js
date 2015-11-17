//conn = new Mongo();
//db = conn.getDB("meteor");
db = connect("localhost:3001/meteor");

keep = ["states", "districts", "markets", "system.indexes"];
collections = db.getCollectionNames();
collections.forEach( function(collectionName) {
    if (keep.indexOf(collectionName) == -1) {
        db[collectionName].drop();
    }    
});
// write drop functions for collections with specific prefix