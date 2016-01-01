const { Dialog, FlatButton, RadioButton, RadioButtonGroup, RefreshIndicator, Toggle } = mui;

SeriesInterface = React.createClass({
    mixins: [ReactMeteorData],

    getInitialState: function() {
        var color = this._getRandomColor([], false);
        var usedColors = [];
        usedColors.push(color);
        var pS = this.props.parentState;
        console.log(pS);
        var pD = this.props.parentDistrict;
        console.log(pD);
        selectedSeries = [];
        // TODO: add statelist and districtlist parts
        // restrict comparions arrays to max length 3
        // set i according to length of selectedSeries before insert
        // ? state, district, market
        var levelObj = this._createLevelObj(this.props.adminUnit, this.props.unitName, this.props.commodity, false, 'avg', color);
        if (pS) {
            // parentState
            // TODO: right now hoping that not the same colors are generated
            color = this._getRandomColor(usedColors, false);
            usedColors.push(color);
            var pSObj = this._createLevelObj('state', this.props.parentState, this.props.commodity, false, 'avg', color);
            pSObj.i = 0; 
            selectedSeries.push(pSObj);
            if (pD) {
                var color = this._getRandomColor(usedColors, false);
                usedColors.push(color);
                pDObj = this._createLevelObj('district', this.props.parentDistrict, this.props.commodity, false, 'avg', color);
                pDObj.i = 1;
                selectedSeries.push(pDObj);
            }
        }

        var length = selectedSeries.length;
        var count = length;

        // TODO: need to make modifications to addForm and RemovalList
        var first = true;
        console.log(this.props.stateList);
        console.log(this.props.districtList);

        if (this.props.stateList.length > 0) {
            var pSObj = null;
            for (i in this.props.stateList) {
                var state = this.props.stateList[i];
                var color = this._getRandomColor(usedColors, false);
                usedColors.push(color);
                if (first) {
                    pSObj = this._createLevelObj('state', state, this.props.commodity, false, 'avg', color);
                    pSObj.i = parseInt(i) + count;
                    first = false;
                } else {
                    var unitObj = this._createUnitObj('state', state, this.props.commodity, false, 'avg', color)
                    pSObj.selected.push(unitObj);
                }
            }
            selectedSeries.push(pSObj);
        }

        if (this.props.districtList.length > 0) {
            var pDObj = null;    
            for (i in this.props.districtList) {
                var district = this.props.districtList[i];
                var color = this._getRandomColor(usedColors, false);
                usedColors.push(color);
                if (first) {
                    pDObj = this._createLevelObj('district', district, this.props.commodity, false, 'avg', color);
                    pDObj.i = parseInt(i) + count;
                    first = false;
                } else {
                    var unitObj = this._createUnitObj('district', district, this.props.commodity, false, 'avg', color);
                    pDObj.selected.push(unitObj);
                }
            }
            selectedSeries.push(pDObj);
        }        

        if (this.props.stateList.length === 0 && this.props.districtList.length === 0) {
            levelObj.i = count;
            selectedSeries.push(levelObj);
        }

        return {
            currAdminUnit: false,
            currUnitName: false,
            currCommodity: false,
            currVariety: 'avg',
            selectedSeries: selectedSeries,
            usedColors: usedColors,
            seriesColor: color
            // color the listitems according to the commodity's color, while the color of the left 3px border represents the level (orange, yellow, green)
        };
    },

    /*
    seriesColor: color,
    selectedSeries: [{
        adminUnit : this.props.adminUnit,
        i: 0,
        selected : [{
            name: this.props.unitName,
            adminUnit : this.props.adminUnit,
            commodity: this.props.commodity,
            raw: false,
            variety: null,
            color: color,
            id: [this.props.adminUnit, this.props.unitName, this.props.commodity].join('.')
        }, {
            name: this.props.unitName,
            adminUnit: this.props.adminUnit,
            commodity: "Wheat",
            k: 1
        }]
    }],
    */

    // NOTE:
    // The component's this.data is initially populated from a componentDidMount callback.
    getMeteorData() {
        // TODO: select commodity and get list of commodities for this.props.unitName, this.props.adminUnit
        // get chart data for predefined commodity name
        // TODO: props are only used for the first fetch
        var currAdminUnit = this.state.currAdminUnit;
        if (!currAdminUnit)
            currAdminUnit = this.props.adminUnit;
        var currUnitName = this.state.currUnitName;
        if (!currUnitName)
            currUnitName = this.props.unitName;
        var currCommodity = this.state.currCommodity;
        var currVariety = this.state.currVariety;

        // NOTE: assumption, either stateList or districtList is passed non-empty
        // if one of these lists is set, adminUnit and unitName are set to last clicked unit
        // is using state doesn't work, use currCommodity
        var chartData = [];
        // distinguish between chartData and initChartData

        var chartDataHandle;
        // TODO: check if logic is sound
        // if commodity has not been set by form
        if (!currCommodity) {
            console.log('passing into init data fetching');
            // NOTE: this conditional block is entered until state is set in addSeries! => do not allow form to be submitted before data is loaded
            // what to do about district, state call?
            var market = this.props.adminUnit === 'market' ? this.props.unitName : null;
            var state = [this.props.parentState];
            var district = [this.props.parentDistrict];
            district = this.props.adminUnit === 'district' ? [this.props.unitName] : district;
            /*if (this.props.adminUnit === 'market') {
                var initChartDataHandle = Meteor.subscribe("initSeriesChartData", [this.props.parentState], [this.props.parentDistrict], this.props.unitName)
            } else if (this.props.adminUnit === 'district') {
                var initChartDataHandle = Meteor.subscribe("initChartDataHandle", [this.props.parentState], this.props.unitName, null)
            }*/
            if (this.props.stateList.length > 0) {
                state = this.props.stateList;
                district = [];
                //var initChartDataHandle = Meteor.subscribe("initChartDataHandle", this.props.stateList, [], null)
            } 
            if (this.props.districtList.length > 0) {
                district = this.props.districtList;
                //var initChartDataHandle = Meteor.subscribe("initChartDataHandle", this.props.parentState, [this.props.parentDistrict], null)
            }
            /*else {
                var chartDataHandle = Meteor.subscribe("seriesChartData", currAdminUnit, currUnitName, currCommodity);
            }*/
            // TODO: getting district and state on top of market only makes sense if there a several markets per district with that commodity
            if (this.props.adminUnit === 'state' && !this.props.comparison) {
                chartDataHandle = Meteor.subscribe("seriesChartData", this.props.adminUnit, this.props.unitName, this.props.commodity);
            } else {
                console.log(state);
                console.log(district);
                console.log(market);
                chartDataHandle = Meteor.subscribe("initSeriesChartData", state, district, market, this.props.commodity);
            }
            // problem now is fetching the data!!!, need to fill the _fetchData procedure to fetch data from minimongo collection
        } else {
            chartDataHandle = Meteor.subscribe("seriesChartData", currAdminUnit, currUnitName, currCommodity);
        }

        // metaData stays the same => uses props passed from app
        var metaDataHandle = Meteor.subscribe("seriesMetaData", currAdminUnit, this.props.parentState, this.props.parentDistrict);

        var data = [];
        // if adminUnit state: chartInitData = chartData else chartInitData = this._fetchChartInitData
        console.log(this.props.comparison);
        console.log(currCommodity);
        if (chartDataHandle.ready()) {
            if (currCommodity || (this.props.adminUnit === 'state' && !this.props.comparison)) {
                // collName?
                console.log('how did I end up here?');
                currCommodity = this.props.adminUnit === 'state' ? this.props.commodity : currCommodity;
                var collName = currAdminUnit.toTitleCase() + currCommodity.toTitleCase();
                if (currVariety !== 'avg')
                    collName = collName + 'Varieties';

                seriesSelector = {};
                seriesSelector[currAdminUnit] = currUnitName;
                console.log(seriesSelector);

                if (chartDataHandle.ready()) {
                    var dataItem = {
                        adminUnit: currAdminUnit,
                        name: currUnitName,
                        commodity: currCommodity,
                        id: [currAdminUnit, currUnitName, currCommodity, currVariety].join('.'),
                        data: window[collName].find(seriesSelector).fetch()
                    };
                    data.push(dataItem);
                }
            } else {
                // TODO: figure out which params to pass
                console.log('fetching init chart data');
                data = chartDataHandle.ready() ? this._fetchChartInitData(state, district, market, this.props.commodity) : [];
            }
            console.log("I'm in but I didn't go anywhere!");
        }
        console.log('checking if data fetched');
        console.log(data);
        console.log(chartDataHandle.ready());

        return {
          // TODO: same data bug is here: find clause needs to specify for which adminUnit to load the data
          metaDataLoading: !metaDataHandle.ready(),
          metaData: metaDataHandle.ready() ? this._fetchMetaData(currAdminUnit) : [],
          chartDataLoading: !chartDataHandle.ready(),
          chartData: data,
          adminUnit: currAdminUnit,
          unitName: currUnitName,
          commodity: currCommodity
        };
    },

    shouldComponentUpdate: function(nextProps, nextState) {
        console.log(this.state);
        console.log(nextState);
        return true;
    },
    
    _getRandomColor: function(usedColors, useState) {
        var color = randomColor(); // {luminosity: 'dark'}
        var useState = false;
        if(!this.state) 
            return color;
        if (useState) {
            usedColors = this.state.usedColors;
            useState = true;
        }
        if (usedColors.indexOf(color) > -1) {
            return useState ? this._getRandomColor([], useState) : this._getRandomColor(usedColors, useState);
        } else {
            return color;
        }       
    },

    _createUnitObj: function(adminUnit, unitName, commodity, raw, variety, color) {
        return {
            name: unitName,
            adminUnit : adminUnit,
            commodity: commodity,
            raw: raw,
            variety: variety,
            color: color,
            id: [adminUnit, unitName, commodity, variety].join('.')      
        };
    },

    _createLevelObj: function(adminUnit, unitName, commodity, raw, variety, color) {
        levelObj = {
            adminUnit : adminUnit,
            selected: [this._createUnitObj(adminUnit, unitName, commodity, raw, variety, color)]
        };
        return levelObj;
    },

    _addSeries: function(item) {
        var selectedSeries = this.state.selectedSeries;
        adminObj = selectedSeries.filter(function(obj) { return obj.adminUnit === item.adminUnit; })[0];
        selectedLength = adminObj.selected.length;
        //item.k = selectedLength;
        // TODO: add raw to id ???
        //var variety = item.variety ? item.variety : 'avg';
        item.id = [item.adminUnit, item.name, item.commodity, item.variety].join('.');
        var usedColors = this.state.usedColors;
        var color = this._getRandomColor([], true);
        usedColors.push(color);
        console.log(color);
        item.color = color;
        adminObj.selected.push(item);
        selectedSeries[adminObj.i] = adminObj;
        console.log('add series state before');
        console.log(this.state);
        console.log('item variety');
        console.log(item.variety);
        this.setState({
           currAdminUnit: item.adminUnit,
           currUnitName: item.name,
           currCommodity: item.commodity,
           currVariety: item.variety,
           seriesColor: color
           //selectedSeries: selectedSeries,
           //usedColors: usedColors
        });
        // add to chart in SeriesChart is automatically called upon data being passed to props
    },

    _removeSeries: function(item) {
        // now how to find series? 
        // can access adminUnit and index (have to make sure index is always in the right order)
        // just loop through?
        // find array index, use splice to remove array entry
        // the validity checks should be done in removallist ==> this method should have removal functionality only
        console.log('trying to remove');
        console.log(item);
        if (this.state.selectedSeries.length > 1 || this.state.selectedSeries[0].selected.length > 1) {
            console.log('passed into conditional block');
            // TODO: need to deep copy?
            var selectedSeries = this.state.selectedSeries;
            var adminObj = selectedSeries.filter(function(obj) { return obj.adminUnit === item.adminUnit; })[0];
            var index = -1;
            console.log(adminObj.i);
            for(var i=0; i < adminObj.selected.length; i++) {
                console.log(i);
                // TODO: this must be wrong right?
                var currSeriesID = adminObj.selected[i].id;
                if (currSeriesID === item.id) {
                    index = i;
                }
            }
            console.log(index);
            adminObj.selected.splice(index, 1);

            var id = [item.id, 'price'].join('.');
            this.refs.chart._removeFromChart(id);
            if (item.variety === 'avg') {
                var id = [item.id, 'tonnage'].join('.');
                this.refs.chart._removeFromChart(id);
            }

            console.log(adminObj.i);
            //this.state.selectedSeries[i].selected = adminObj.selected
            selectedSeries[adminObj.i] = adminObj;
            // NOTE: state is not automatically updated with manipulation here
            this.setState({
               selectedSeries: selectedSeries
            });
        }
    },

    _getDataItem: function(adminUnit, unitName, commodity, state) {
        var collName = titlelize(adminUnit)+commodity.replace(" ", "");
        console.log(collName);
        console.log(window[collName]);
        var selector = {};
        selector[adminUnit] = unitName;
        if (state !== null) {
            selector['state'] = state;
        }
        console.log(selector);
        var dataItem = {
            adminUnit: adminUnit,
            name: unitName,
            commodity: commodity,
            id: [adminUnit, unitName, commodity, 'avg'].join('.'),
            data: window[collName].find(selector).fetch()
        };
        console.log(dataItem);
        return dataItem;
    },

    // TODO: meta data has to match chart data for compare feature and district compare feature
    // ==> implement similar calls in parallel
    // but how to match metaData element and chartData element
    // how to assign color: color is passed to SeriesChart.. meaning that there is no way of determining to which data color belongs (color mismatch)
    // REACTIVE JOINS cursor issue: https://www.discovermeteor.com/blog/reactive-joins-in-meteor/
    _fetchChartInitData: function(stateList, districtList, market, commodity) {
        var data = [];
        // fuck this shit, I published weird docs into the collections..
        // doesnt change query
        sLength = stateList.length;
        dLength = districtList.length;
        var stateName = stateList[0];
        var districtName = districtList[0];
        var data = [];
        // TODO: assert sLength can't be 0
        if (sLength === 1) {
            var dataItem = this._getDataItem('state', stateName, commodity, null);
            data.push(dataItem);
        } else {
            // sLength series (direct comparison)   
            for (i in stateList) {
                var stateName = stateList[i];
                var dataItem = this._getDataItem('state', stateName, commodity, null);
                data.push(dataItem);
            }
        }
        // NOTE: dLength can be 0 ==> do nothing
        if (dLength === 1) {
            // one state, one district
            var dataItem = this._getDataItem('district', districtName, commodity, stateName);
            data.push(dataItem);
        } else if (dLength > 1) {
            // one state, dLength districts (direct comparison)
            for (i in districtList) {
                var districtName = districtList[i];
                var dataItem = this._getDataItem('district', districtName, commodity, stateName);
                data.push(dataItem);
            }
        }
        console.log(market);
        if (market !== null) {
            var dataItem = this._getDataItem('market', market, commodity, stateName);
            data.push(dataItem);
            // one state series, one district series, one market series 
        } 
        return data;
    },   

    _fetchMetaData: function(currAdminUnit) {
        console.log(this.props.parentState);
        console.log(this.props.parentDistrict);
        if (currAdminUnit === 'state') {
            var data = [
                States.find({}, { fields: { name: 1, commodities: 1 } }).fetch()
            ];
        // district: get all districts in parent state and parent state
        } else if (currAdminUnit === 'district') {
            var data = [
                States.find({ _id : this.props.parentState }, { fields: { name: 1, commodities: 1 } }).fetch(),
                Districts.find({ state : this.props.parentState }, { fields: { name: 1, commodities: 1 } }).fetch()
            ] ;
        // market: get all markets in parent district as well as parent district and parent state
        } else if (currAdminUnit === 'market') {
            var data = [
                States.find({ _id: this.props.parentState }, { fields : { name: 1, commodities: 1 } }).fetch(),
                Districts.find({ state: this.props.parentState, name: this.props.parentDistrict }, { fields: { name: 1, commodities: 1 }}).fetch(),
                // TODO: add commodities field to markets (do in aggregate.py)
                Markets.find({ state: this.props.parentState, district: this.props.parentDistrict }, { fields: { name: 1, commodities: 1 }}).fetch()
            ];
        }
        console.log('metadata');
        console.log(data);
        if (data) return data;
        else return [];
    },

    _toggleGrouping: function(e, toggled) {
        this.refs.chart._setGrouping(toggled);
    },

    _togglePoints: function(e, toggled) {
       this.refs.chart._setMarkers(toggled);
    },

    render: function() {
        //<Add/>
        //<SeriesChart/>
        if (this.props.comparison) {
            var titleName = "Comparison of ";
            if (this.props.stateList.length > 0) {
                titleName = titleName + "States: " + this.props.stateList.join(', ') + " for " + this.props.commodity;
            } else if (this.props.districtList.length > 0) {
                titleName = titleName + "Districts: " + this.props.districtList.join(', ') + " for " + this.props.commodity;
            }
        } else {
            var titleName = this.props.unitName+" ("+this.props.adminUnit+")";
        }
        let interfaceActions = [
            <FlatButton
            key={1}
            label="Close"
            primary={true}
            onTouchTap={this.props.onClose} />
        ];
        var width = $(document).width() * .8 * .75 * .5;
        var loadingLayer = this.data.chartDataLoading ? <RefreshIndicator size={30} top={250} left={width-40} status="loading" /> : '';
        // <Add selectable={this.data.metaData} selected={this.state.selectedSeries} />
        //<Toggle ref="pointToggle" name="pointToggle" className="toggle" value="points" label=" point markers" defaultToggled={true} onToggle={this._togglePoints} />
        return ( 
            <Dialog
              title={titleName}
              actions={interfaceActions}
              open={true}
              contentClassName="overlay"
              autoDetectWindowHeight={true}
              autoScrollBodyContent={true}
              style={{
                zIndex: 1000
              }}>
              <AddForm loading={this.data.chartDataLoading} selectable={this.data.metaData} displayable={this.state.selectedSeries} add={this._addSeries} adminUnit={this.props.adminUnit} unitName={this.props.unitName} commodity={this.data.commodity} />
              <Toggle ref="groupToggle" name="groupToggle" className="toggle" value="grouped" label=" data grouping" defaultToggled={true} onToggle={this._toggleGrouping} />
              <SeriesChart ref="chart" loading={this.data.chartDataLoading} data={this.data.chartData} selected={this.state.selectedSeries} color={this.state.seriesColor} adminUnit={this.data.adminUnit} unitName={this.data.unitName} commodity={this.data.commodity} />

              {loadingLayer}
              <RemovalList selected={this.state.selectedSeries} onDelete={this._removeSeries} />
            </Dialog>
        );
    }
});
/*
<RadioButtonGroup name="isImputed" defaultSelected="original">
  <RadioButton
    value="original"
    label="display original data"
    style={{marginBottom:16}} />
  <RadioButton
    value="imputed"
    label="display imputed data"
    style={{marginBottom:16}}/>
</RadioButtonGroup>
<RadioButtonGroup name="isAveraged" defaultSelected="mean">
  <RadioButton
    value="mean"
    label="display mean price"
    style={{marginBottom:16}} />
  <RadioButton
    value="varieties"
    label="display variety prices"
    style={{marginBottom:16}}/>
</RadioButtonGroup>
*/