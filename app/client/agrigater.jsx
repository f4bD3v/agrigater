if (Meteor.isClient) {
// TODO: change once there is support, issue: https://github.com/meteor/react-packages/issues/123
//var ReactDOM = Meteor.require("react-dom");
  Meteor.startup(function () {
    // Use Meteor.startup to render the component after the page is ready
    ReactDOM.render(<App />, document.getElementById("agrigater"));
  });
}
