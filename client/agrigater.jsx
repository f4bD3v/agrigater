if (Meteor.isClient) {
  States = new Mongo.Collection("states");
  Districts = new Mongo.Collection("districts");
  Markets = new Mongo.Collection("markets");

  Meteor.startup(function () {
    // Use Meteor.startup to render the component after the page is ready
    React.render(<App />, document.getElementById("container"));
  });
}
