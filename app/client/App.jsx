/*
  Documentation:
    - great comparison between react and blaze: https://themeteorchef.com/blog/react-vs-blaze/ (also good source for correct code and practices!)

  Building a (small) complete App:
    - https://facebook.github.io/react/docs/thinking-in-react.html 

  React style guide:
    - https://web-design-weekly.com/2015/01/29/opinionated-guide-react-js-best-practices-conventions/

  Execution Sequence of React Component's Lifecycle:
    - http://javascript.tutorialhorizon.com/2014/09/13/execution-sequence-of-a-react-components-lifecycle-methods/
*/
// App component - represents the whole app
const { RefreshIndicator, Dialog, Select } = mui;

App = React.createClass({
  // This mixin makes the getMeteorData method work
  propTypes: {}, // validation & and useful documentation for each prop the component will receive
  mixins: [ReactMeteorData],

  getInitialState: function() {
    var thresholds = Object.create(null);
    thresholds["coverage"] = 0;
    thresholds["missingPrices"] = 100;
    thresholds["missingTonnages"] = 100;
    return {
      visMode: 'timeSeries',
      showVis: false,
      markets: true,
      zoomed: false,
      prompt: false,
      adminUnit: 'country',
      unitName: 'India',
      parentState: null,
      parentDistrict: null,
      stateList: [],
      districtList: [],
      marketScope: 'India',
      commodity: 'all',
      showNav: false,
      rangeStart: new Date(2005, 1, 1),
      rangeEnd: new Date(2015, 9, 1),
      thresholds: thresholds,
      comparison: false
    };
  },
  // very probably is not needed in top level component
  getDefaultProps: function() {},

  componentDidMount: function() {
    this.tempRangeStart = null;
  },

  /*
    IMPORTANT: for double rendering problem https://forums.meteor.com/t/whats-the-preferred-way-to-initialize-data-in-getmeteordata/8635
    REFACTOR ALL OF THE CODE WITH CONDITONAL SUBSCRIPTIONS, only subscribe in respective modes
  */ 
  getMeteorData() {
    // first thing the app loads is the mapLayers synchronously
    // it's fine the make the subscription unconditional, since nothing is updated..
    var layersHandle = Meteor.subscribe('getMapLayers'); 

    console.log(this.state.adminUnit);
    console.log(this.state.unitName);
    console.log(this.state.marketScope);

    var unitData = [];
    unitDataLoading = false;
    var marketData = [];
    marketDataLoading = false;

    // TOOD: if commodity !== 'all' get aggregates from a 'total-trade' collection across commodities
    if (this.state.visMode === 'trade' && this.state.commodity !== 'all') { // avoid having to provide totals across commodities
      // TODO: add code for districtLevel aggregates
      var level = 'state';
      if (this.state.adminUnit === 'state')
        level = 'district';
      var aggregatesHandle = Meteor.subscribe('getTonnageAggregates', level, this.state.commodity, this.state.rangeStart, this.state.rangeEnd, this.state.unitName);
      unitDataLoading = ! aggregatesHandle.ready();
      if (aggregatesHandle.ready()) {
        if (this.state.adminUnit === 'state') 
          unitData = AggregateTonnage.find({ commodity: this.state.commodity, state: this.state.unitName }).fetch();
        else
          unitData = AggregateTonnage.find({ commodity: this.state.commodity }).fetch();
      }
      // NOTE: no market data
    }

    // NOTE: do not load districts if vis is being opened, do not resetStyles, do not remove market pins ==> easyfix: include !this.state.showVis

    // TODO: when to use this.state.commodity !== 'all'? need to reset also
    if (this.state.visMode === 'timeSeries' && !this.state.showVis) {
      if (this.state.adminUnit === 'country' && this.state.unitName === 'India' && !this.state.zoomed && this.state.commodity !== 'all') {
        if (this.state.thresholds.coverage === 0 && this.state.thresholds.missingPrices === 100 && this.state.thresholds.missingTonnages === 100) {
          var displayedStatesHandle = Meteor.subscribe('getStatesByCommodity', this.state.commodity);
          unitDataLoading = ! displayedStatesHandle.ready();
          if (displayedStatesHandle.ready())
            unitData = States.find({ commodity: this.state.commodity }).fetch(); // with projections: , { fields: { state: 1 } }
        } else {
          var stateStatsHandle = Meteor.subscribe('stateStatistics', this.state.commodity, this.state.thresholds, this.state.rangeStart, this.state.rangeEnd);
          unitDataLoading = ! stateStatsHandle.ready();
          if (stateStatsHandle.ready())
            unitData = StateStatistics.find({ commodity: this.state.commodity }).fetch();
        }
      // VS. simply if this.state.zoomed? ==> get state variables sorted out man!
      // TODO: if this.state.zoomed? then why the fuck does it show the districts of a state after opening seriesInterface
      } else if (this.state.adminUnit === 'state' && this.state.zoomed && this.state.commodity !== 'all') {
        if (this.state.thresholds.coverage === 0 && this.state.thresholds.missingPrices === 100 && this.state.thresholds.missingTonnages === 100) {
          var displayedDistrictsHandle = Meteor.subscribe('getDistrictsByCommodity', this.state.commodity, this.state.unitName);
          console.log(this.state.adminUnit);
          unitDataLoading = ! displayedDistrictsHandle.ready();
          if (displayedDistrictsHandle.ready())
            unitData = Districts.find({ commodity: this.state.commodity, state: this.state.unitName }).fetch(); // , { fields: { state: 1, districts: 1 } }
          console.log(unitData);
        } else {
          var districtStatsHandle = Meteor.subscribe('districtStatistics', this.state.unitName, this.state.commodity, this.state.thresholds, this.state.rangeStart, this.state.rangeEnd);
          unitDataLoading = ! districtStatsHandle.ready();
          if (districtStatsHandle.ready())
            unitData = DistrictStatistics.find({ commodity: this.state.commodity, state: this.state.unitName }).fetch();
        }
      }
      // TODO: scoping of market data only works when thresholds are non-default
      console.log('markets active?');
      console.log(this.state.markets);
      if (this.state.markets) {
        if (this.state.thresholds.coverage === 0 && this.state.thresholds.missingPrices === 100 && this.state.thresholds.missingTonnages === 100) {
          console.log('init thresholds');
          console.log(this.state.marketScope);
          var displayedMarketsHandle = Meteor.subscribe('getMarketsByCommodity', this.state.commodity, this.state.marketScope);
          marketDataLoading = ! displayedMarketsHandle.ready();
          if (displayedMarketsHandle.ready()) {
            console.log(this.state.commodity);
            if (this.state.commodity !== 'all') {
              // take only markets in state: how to that properly?
              selector = {
                'commodities.commodity' : this.state.commodity
              };
              if (this.state.marketScope !== 'India') {
                selector.state = this.state.marketScope;
              }
              marketData = Markets.find(selector, { fields: { state: 1, district: 1, market: 1 } }).fetch();
            } else {
              // error here: I'm not scoping => need to find out first?
              selector = {};
              if (this.state.marketScope !== 'India') {
                selector.state = this.state.marketScope;
              }
              marketData = Markets.find(selector, { fields: { state: 1, district: 1, market: 1 } }).fetch();
            }
            console.log(marketData);
          }
        } else {
          console.log('custom threshold');
          console.log(this.state.unitName);
          console.log(this.state.marketScope);
          console.log(this.state.commodity);
          var marketStatsHandle = Meteor.subscribe('marketStatistics', this.state.unitName, this.state.commodity, this.state.thresholds, this.state.rangeStart, this.state.rangeEnd);
          marketDataLoading = ! marketStatsHandle.ready();
          // state is an additional non-collection field introduced in publication to differentiate between different scopes (India, State)
          console.log('ready?');
          console.log(marketStatsHandle.ready());
          if (marketStatsHandle.ready()) {
            if (this.state.unitName === 'India')
              marketData = MarketStatistics.find({ commodity: this.state.commodity }).fetch();
            else
              marketData = MarketStatistics.find({ commodity: this.state.commodity, state: this.state.marketScope }).fetch();
          }
          console.log(marketData);
        }
      }
    }

    var districtLayer = [];
    if (this.state.adminUnit === 'state' && this.state.zoomed) {
      var districtLayerHandle = Meteor.subscribe('getStateDistricts', this.state.unitName);
      if (districtLayerHandle.ready())
        // only one districts topology should be published to minimongo at any given time
        districtLayer = DistrictTopology.find({ state: this.state.unitName }).fetch();
    }

    var commoditiesHandle = Meteor.subscribe('getCommodities');
    /*
    var commodities = [];
    var commoditiesLoading = false;
    if (this.visMode === 'timeSeries') {
      var commoditiesHandle = Meteor.subscribe('getCommodities');
      commoditiesLoading = ! commoditiesHandle.ready();
      if (commoditiesHandle.ready())
    }
    */
    /*
      Alternatively change props passed to ownees bassed on visualization mode.. Will that make it easier to handle state in ownees?
    */ 
    return {
      layersLoading : ! layersHandle.ready(),
      layers: layersHandle.ready() ? MapLayers.find().fetch(): [],
      commoditiesLoading: ! commoditiesHandle.ready(),
      commodities: commoditiesHandle.ready() ? States.find({ _id : "India"}, { fields: { commodities: 1 } }).fetch() : [],
      districtLayer: districtLayer,
      unitData: unitData,
      marketData: marketData,
      unitDataLoading: unitDataLoading,
      marketDataLoading: marketDataLoading
      //loadingDisplayed: ! (displayedStatesHandle.ready() && displayedDistrictsHandle.ready() && displayedMarketsHandle.ready()),
    };
  },

  _handleResetZoom: function() {
    this.setState({
      zoomed: false,
      adminUnit: 'country',
      unitName: 'India',
      marketScope: 'India',
      parentState: null,
      parentDistrict: null
    }); 
  },

  _updateSelection: function(adminUnit, unitName, add) {
    // make sure adminUnit is either State or District
    var selection = adminUnit === 'State' ? this.state.stateList : this.state.districtList;
    if (add) {
      selection.push(unitName);
    } else {
      var i = selection.indexOf(unitName);
      if (i > -1)
        selection.splice(i, 1);
    }
    if (adminUnit === 'State') {
      this.setState({
        stateList: selection
      });
    } else {
      // districtList
      this.setState({
        districtList: selection
      });
    }
  },

  _resetSelection: function() {
    // TODO: how to decide?
    var selection = this.state.zoomed ? this.state.districtList : this.state.stateList;
    console.log(selection);
    if (selection.length > 0) {
      this.refs.map._resetSelectionStyles();
      selection = [];
      if (!this.state.zoomed) {
        this.setState({
          stateList: selection
        });
      } else {
        // districtList
        this.setState({
          districtList: selection
        });
      }
    }
  },

  // NOTE: will probably need further map event handlers
  _handleMapClick: function(featureProps, zooming, market) {
    // TODO: comparisons
    // what information is needed to successfully open seriesInterface:
    // set title to: "Comparison of {[units].join(, )} for {commodity}
    // set selected series and load data
    // prompt the user if too many units selected for comparison
    // upon close remove selection of units
    var stateLevel = featureProps.level === "State";
    var adminUnit = market ? "market" : featureProps.level.toLowerCase();
    // TODO: specifically handle district click, interplay between zoomed and showVis 
    if (zooming) {
      this.setState({
        zoomed: true,
        adminUnit: adminUnit,
        unitName: featureProps.name,
        parentDistrict: market ? featureProps.district : null, 
        parentState: stateLevel ? null : featureProps.state,
        marketScope: stateLevel ? featureProps.name : 'India'
      });
    } else {
      if (this.state.commodity === 'all') {
        // open dialog
        console.log('cant open seriesInterface in this mode');
        this.setState({
          prompt: true
        });
        return;
      }
      market = market || false;   
      console.log(market);
      console.log('district click')
      console.log(adminUnit);
      console.log(featureProps)
      this.setState({
        // what to do about visualization type?
        showVis: true,
        adminUnit: adminUnit,
        unitName: featureProps.name,
        parentDistrict: market ? featureProps.district : null, 
        parentState: stateLevel ? null : featureProps.state
      });
    }
  },

  _handleCommoditySelection: function(commodity) {
    var commodity = commodity;
    console.log(commodity);
    // TODO: states layers need to have their styles reset, what about markets?
    if (commodity === 'All Commodities') {
      this.refs.map._resetStyle("States");
      commodity = 'all';
    }
    console.log(commodity);
    this.setState({
      commodity: commodity,
      prompt: false,
      showNav: false 
    })
  },

  _handleNavHide: function() {
    if (this.state.showNav) {
      this.setState({
        showNav: false
      });
      return;
    }
    console.log('show nav state should be positive here!');
  },

  _handleVisChange: function(visMode) {
    console.log(visMode);
    // TODO: WHAT IF I WANT TO CHECK ANOTHER COMMODITY IN THE SAME RANGE ==> do not change rangeStart and rangeEnd for a simple commodity selection (only for vis change)
    if (!this.state.showNav) {
      console.log('lets do this');
      // map markers etc
      // reset state to initial state
      ts = (visMode === 'timeSeries');
      var showNav = !(this.state.visMode !== visMode && this.state.commodity !== 'all');
      var rangeStart = (visMode === 'trade') ? new Date(2015, 3, 1) : new Date(2005, 1, 1);
      this.setState({
        visMode: visMode,
        showVis: !ts,
        markets: ts,
        showNav: showNav,
        rangeStart: rangeStart,
        rangeEnd: new Date(2015, 9, 1)
      });
    }
  },

  _handleCloseVis: function() {
    // TODO: if it was a comparison popup reset values of all states
    var adminUnit = this.state.zoomed ? 'state' : 'country';
    var unitName = this.state.zoomed ? this.state.marketScope : 'India';
    if (this.state.comparison) {
      this._resetSelection();
      this.setState({
        showVis: false,
        adminUnit: adminUnit,
        unitName: unitName,
        parentState: null,
        parentDistrict: null
      });
    } else {
      this.setState({
        showVis: false,
        adminUnit: adminUnit,
        unitName: unitName,
        parentState: null,
        parentDistrict: null
      });
    }
  },

  _handleThresholdChange: function(value, thresholdType) {
    var thresholds = this.state.thresholds;
    console.log(thresholdType);
    thresholds[thresholdType] = value; 
    this.setState({
      thresholds: thresholds
    });
  },

  _handleDateRangeSet: function(rangeStart, rangeEnd) {
    this.setState({
      rangeStart: rangeStart,
      rangeEnd: rangeEnd,
    });
  },

  _handleDateRangeChange: function(date, handle) {
    console.log(date);
    if (handle === 0) {
      //this.tempRangeStart = date;
      this.setState({
        rangeStart: date,
      });
    } else if (handle > 0) {
      console.log(this.tempRangeStart);
      /*
      this.setState({
        rangeStart: this.tempRangeStart,
        rangeEnd: date,
        tempRangeEnd: null
      });
      */
      this.setState({
        rangeEnd: date,
      });
    }
  },

  _handlePromptClose: function() {
    this.setState({ prompt: false });
  },

  _switchComparison: function(isChecked) {
    console.log(this.state.comparison);
    console.log(isChecked);
    if (this.state.comparison) {
      this.setState({
        comparison: isChecked
      });
      this._resetSelection();
    } else {
      this.setState({
        comparison: isChecked
      });
    }
  },

  render: function() {
    // TODO: render menu on top of map, render aknowledgement section on bottom of page (Agmarknet, MongoDB, Meteor, React, Pandas, Blaze, Requests, Google Geocoding, Python)
    //return this.renderMap(); //==> should print a map div and connect it with leaflet
    var compare = (this.state.visMode === 'timeSeries' && this.state.commodity !== 'all')
    var type = (this.state.visMode === 'timeSeries' && this.state.comparison) ? 'comparison' : 'inspection';
    var seriesInterface = (this.state.showVis && this.state.visMode === 'timeSeries') ? <SeriesInterface adminUnit={this.state.adminUnit} unitName={this.state.unitName} commodity={this.state.commodity} parentState={this.state.parentState} parentDistrict={this.state.parentDistrict} comparison={this.state.comparison} stateList={this.state.stateList} districtList={this.state.districtList} onClose={this._handleCloseVis} /> : '';
    var prompt = (this.state.prompt) ? <Dialog
              title={'User Notice'}
              open={true}
              onRequestClose={this._handlePromptClose}
              style={{
                zIndex: 1000
              }}>{'Please click on the "Time Series" button at the top and then select a commodity from the navigation on the left!'}<br/>{'To close you may click outside the dialog..'}</Dialog> : '';
    var offsetTop = $(document).height() * .45;
    var offsetLeft = $(document).width() * .45;
    // using material ui theme
    var loading = (this.data.layersLoading || this.data.unitDataLoading || this.data.marketDataLoading || this.data.commoditiesLoading) ? <RefreshIndicator size={40} left={offsetLeft} top={offsetTop} loadingColor={"#FF6600"} className="indicator" status="loading" /> : '';

    var selection = this.state.zoomed ? this.state.districtList : this.state.stateList;
    console.log(this.state.rangeStart);
    console.log(this.state.rangeEnd);
    // only show range slider when not init Thresholds
    var rangeSlider = (this.state.commodity === 'all' || this.state.showNav || (this.state.visMode === 'timeSeries' && this.state.thresholds.coverage === 0 && this.state.thresholds.missingPrices === 100 && this.state.thresholds.missingTonnages === 100)) ? '' : <RangeSlider ref="rangeSlider" rangeStart={this.state.rangeStart} rangeEnd={this.state.rangeEnd} onSet={this._handleDateRangeSet} onDrag={this._handleDateRangeChange}/>;
    var qualityControls = (this.state.visMode === 'timeSeries' && this.state.commodity !== 'all') ? <QualityControls changeThreshold={this._handleThresholdChange} /> : '';
    // if ! layersHandle => display loadingj
    //<Map layers={this.data.layers} aggregates={this.data.aggregates} commodity={this.state.commodity} loadingToDisplay={this.data.loadingDisplayed} statesToDisplay={this.data.displayedStates} districtsToDisplay={this.data.displayedDistricts} marketsToDisplay={this.data.displayedMarkets} displayMarketLayer={this.state.markets} districtsLayer={this.data.districtsLayer} adminUnit={this.state.adminUnit} unitName={this.state.unitName} onClick={this._handleMapClick} onReset={this._handleResetZoom} />
    // TODO: look into react-leaflet as map functionality increases in complexity
    return (
      /* as long as loading display empty tiles */
      <div id="container">
        <VisModeControls onSelect={this._handleUserMenuSelection} onCheck={this._switchComparison} setVisType={this._handleVisChange} checkbox={compare} />
        <CommodityControls ref="commControl" open={this.state.showNav} loading={this.data.commoditiesLoading} commodities={this.data.commodities} visMode={this.state.visMode} handleHide={this._handleNavHide} handleSelection={this._handleCommoditySelection} />
        { loading }
        <Map ref="map" layers={this.data.layers} districtLayer={this.data.districtLayer} visMode={this.state.visMode} commodity={this.state.commodity} comparison={this.state.comparison} updateSelection={this._updateSelection} selection={selection} unitData={this.data.unitData} unitDataLoading={this.data.unitDataLoading} markets={this.state.markets} marketData={this.data.marketData} marketDataLoading={this.data.marketDataLoading} adminUnit={this.state.adminUnit} unitName={this.state.unitName} onClick={this._handleMapClick} onReset={this._handleResetZoom} />
        { seriesInterface }
        { prompt }
        { rangeSlider }
        { qualityControls }
      </div>
    );
  }
});