// App component - represents the whole app
App = React.createClass({
  // This mixin makes the getMeteorData method work
  mixins: [ReactMeteorData],
  // Loads items from the Tasks collection and puts them on this.data.tasks
  getMeteorData() {
    Meteor.subscribe('india');
    statesHandle = Meteor.subscribe('states');
    Meteor.subscribe('markets');

    return {
      /*statesReady: statesHandle.ready(),
      india: States.find({ '_id' : 'India'}).fetch(),
      states: States.find({}).fetch()*/
      markets: Markets.find({}).fetch()
      //districts: Districts.find({}).fetch(),
      //markets: Markets.find({}).fetch()
    };
  },
 
  /*
  renderTasks() {
    // NEED TO RENDER GEOJSON ON MAP
    // Get tasks from this.data.tasks
    return this.data.tasks.map((task) => {
      return <Task key={task._id} task={task} />;
    });
  },*/

  // renderMap + renderSomething on map
  renderMap() {
      // pass data to map somehow
      //console.log(this.data.states[0])
      /*
      if (this.data.statesReady) {
        console.log('data ready!')
        console.log(this.data.states[0]);
        console.log(this.data.india[0]);
      } else {
        indiaShape = null
      }*/     
      //return <Map india={this.data.india[0]} />;
      return <Map />;
  },

  render() {
    return this.renderMap(); //==> should print a map div and connect it with leaflet
  }
});