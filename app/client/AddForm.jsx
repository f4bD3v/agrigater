const { RaisedButton, Toggle, Checkbox } = mui

AddForm = React.createClass({
    getDefaultProps: function() {
        return {
            valToIndex: {
                state: 0,
                district: 1,
                market: 2
            }
        }
    },

    getInitialState: function() {
        return {
            addColor: '#ff6600',
            selectAdminUnit: true,
            adminUnit: 'Select admin. unit..',
            unitName: 'Select unit name..',
            commodity: 'Select commodity..',
            variety: 'Select variety..',
            rawData: false,
            selectUnitName: false,
            selectCommodity: false,
            selectVariety: false,
            rawDataToggle: false,
            submitable: false
        };
    },

    _getAdminUnitOptions: function() {
        /*
        var options = [
            { value: 'State', label: 'State' },
            { value: 'District', label: 'District' },
            { value: 'Market', label: 'Market' }
        ];*/
        var options = [
            { value: 'state', label: 'State' }
        ];
        console.log(this.props.selectable);
        selectLength = this.props.selectable.length;
        if (selectLength > 1) {
            options.push({ value: 'district', label: 'District'})
        }        
        if (selectLength > 2) {
            options.push({ value: 'market', label: 'Market'})
        }
        return options;
    },

    _getNamesAndCommodities: function() {
        var adminUnit = this.state.adminUnit;
        if (adminUnit === 'Select admin. unit..')
            adminUnit = this.props.adminUnit;
        var namesAndCommodities = [];
        if (this.props.selectable.length > 0) {
            console.log(adminUnit);
            console.log(this.props.valToIndex[adminUnit]);
            namesAndCommodities = this.props.selectable[this.props.valToIndex[adminUnit]];
        }
        console.log('namesAndCommodities');
        console.log(namesAndCommodities);
        return namesAndCommodities;
    },

    _getUnitNameOptions: function() {
        /*var options = [
            { value: 'Rajasthan', label: 'Rajasthan' },
            { value: 'Madhya Pradesh', label: 'Madhya Pradesh' },
            { value: 'Maharashtra', label: 'Maharashtra' }
        ];*/
        var options = this._getNamesAndCommodities();
        console.log(options);
        if (options.length > 0)
            options = options.map(function(obj) { return { value: obj.name, label: obj.name }; });
        return options;
    },

    _getDisplayedCommodities: function() {
        var adminUnit = this.state.adminUnit;
        var unitName = this.state.unitName;
        if (adminUnit === 'Select admin. unit..')
            adminUnit = this.props.adminUnit;
        if (unitName === 'Select unit name..')
            unitName = this.props.unitName;
        var adminUnitObj = this.props.displayable.filter(function(obj) { return obj.adminUnit === adminUnit; })[0];
        var unitNameObjs = adminUnitObj.selected.filter(function(obj) { return obj.name === unitName; });
        displayedCommodities = [];
        for (var objIndex in unitNameObjs) {
            displayedCommodities.push(unitNameObjs[objIndex].commodity);
        }
        return displayedCommodities;
    },

    _getDisplayedVarieties: function() {
        var adminUnit = this.state.adminUnit;
        if (adminUnit === 'Select admin. unit..')
            adminUnit = this.props.adminUnit;
        var unitName = this.state.unitName;
        if (unitName === 'Select unit name..')
            unitName = this.props.unitName;
        var adminUnitObj = this.props.displayable.filter(function(obj) { return obj.adminUnit === adminUnit; })[0];
        console.log(adminUnitObj);
        console.log(this.state.commodity)
        var unitNameObjs = adminUnitObj.selected.filter(function(obj) { return (obj.name === unitName && obj.commodity === this.state.commodity); });
        console.log(unitNameObjs);
        displayedVarieties = [];
        for (var objIndex in unitNameObjs) {
            displayedVarieties.push(unitNameObjs[objIndex].variety);
        }
        console.log(displayedVarieties);
        return displayedVarieties;
    },

    _getCommodityOptions: function() {
        /*var options = [
            { value: 'Rice', label: 'Rice (Cereal)' },
            { value: 'Wheat', label: 'Wheat (Cereal)' },
            { value: 'Sorghum', label: 'Sorghum (Cereal)' }
        ];*/
        var options = [];
        var unitName = this.state.unitName;
        if (unitName !== 'Select unit name..') {
            //if (unitName === 'Select unit name..')
            //    unitName = this.props.unitName;
            var namesAndCommodities = this._getNamesAndCommodities();
            if (namesAndCommodities.length > 0) {
                // DONE: here the problem is the _id field in state collection => create name field
                var obj = namesAndCommodities.filter(function(obj) { return obj.name === unitName; })[0];
                var displayedCommodities = this._getDisplayedCommodities();
                var comms = obj.commodities;
                console.log(comms);
                //for(var cat in catcomms) {
                //    var comms = catcomms[cat];
                for(var obj in comms) {
                    var comm = comms[obj].commodity;
                    var cat = comms[obj].category;
                    //for (var i in comms) {
                    //   var comm = comms[i];
                    if (displayedCommodities.indexOf(comm) === -1) {
                        var label = comm+' ('+cat+')';
                        options.push({ value: comm, label: label});
                    }                   
                //}
                }
            }
        }
        return options;
    },

    _getVarietyOptions: function() {
        var options = [
            { value: 'avg', label: 'Averaged' },
        ];
        var unitName = this.state.unitName;
        if (unitName !== 'Select unit name..') {
            // names and commodities is: unit names and commodities 
            var namesAndCommodities = this._getNamesAndCommodities();
            console.log(namesAndCommodities);
            console.log(this.state.commodity);
            if (this.state.commodity) {
                if (namesAndCommodities.length > 0) {
                    console.log(unitName);
                    var obj = namesAndCommodities.filter(function(obj) { return obj.name === unitName; })[0];
                    var displayedVarieties = this._getDisplayedVarieties();
                    console.log(displayedVarieties);
                    var commsObjArr = obj.commodities;
                    console.log(commsObjArr);
                    for(var objIndex in commsObjArr) {
                        if (commsObjArr[objIndex].commodity === this.state.commodity) {
                            var varieties = commsObjArr[objIndex].varieties;
                            console.log(varieties);
                            for (var varIndex in varieties) {
                                var variety = varieties[varIndex];
                                if (displayedVarieties.indexOf(variety) === -1) {
                                    options.push({ value: variety, label: variety});
                                }
                            }
                        }
                    }
                }
            }
        }

        /*var options = [
            { value: 'Basmati', label: 'Basmati' },
            { value: 'Fine', label: 'Fine'},
            { value: 'Medium', label: 'Medium' }
        ]*/
        return options;
    },

    _handleSelection: function(selectType, value) {
        // TODO: set and reset values
        if (selectType === 'adminUnit') {
            var color = "#ff6600";
            if (value === 'district') {
                color = "#ffffbf";
            } else if (value === 'market') {
                color = "#91cf60";
            }
            this.setState({
                addColor: color,
                adminUnit: value,
                selectUnitName: true,
                selectAdminUnit: false
            });
        } else if (selectType === 'unitName') {
            this.setState({
                unitName: value,
                selectCommodity: true,
                selectUnitName: false
            });
        } else if (selectType === 'commodity') {
            this.setState({
              commodity: value,
              selectVariety: true,
              selectCommodity: false,
            });
            /*
            if (this.state.adminUnit === 'market') {
                this.setState({
                    commodity: value,
                    selectVariety: true,
                    selectCommodity: false,
                });
            } else {
                this.setState({
                    commodity: value,
                    //rawDataToggle: true,
                    selectCommodity: false,
                    submitable: true
                });
            }*/
        } else if (selectType === 'variety') {
            this.setState({
                //rawDataToggle: true,
                variety: value,
                selectVariety: false,
                submitable: true
            }); 
        }
    },

    _handleToggle: function(e, toggled) {
        console.log(toggled);
        if (toggled) {
            this.setState({
                rawData: true,
            }); 
        } else {
            this.setState({
                rawData: false,
            });
        }
    },

    _handleClear: function() {
        this.replaceState(this.getInitialState());
    },

    _handleAdd: function() {
        seriesItem = {
            adminUnit : this.state.adminUnit,
            name: this.state.unitName,
            commodity: this.state.commodity,
            variety: this.state.variety,
            raw: this.state.rawData
        };
        this._handleClear();
        this.props.add(seriesItem);
    },

    render: function() {
        // change state => load next select field
        // create options first level of metadata array

        // how to preset adminUnit select
        // TODO: bg color add series button depending on adminUnitSelect (orange, beige, green)

        // <Toggle name="rawDataToggle" className="toggle" value="rawData" label=" raw data" disabled={!this.state.rawDataToggle} onToggle={this._handleToggle} />
        return (
            <div className="add-form">
                <h4>Add a Series</h4>
                <div className="select-container">
                    <ReactSelect ref="adminUnitSelect" name="adminUnitSelect" value={this.state.adminUnit} options={this._getAdminUnitOptions()} disabled={!this.state.selectAdminUnit} onChange={this._handleSelection.bind(this, 'adminUnit')} />
                </div>
                <div className="select-container">
                    <ReactSelect ref="unitNameSelect" name="unitNameSelect" value={this.state.unitName} options={this._getUnitNameOptions()} disabled={!this.state.selectUnitName} onChange={this._handleSelection.bind(this, 'unitName')} />
                </div>
                <div className="select-container">
                    <ReactSelect ref="commoditySelect" name="commoditySelect" value={this.state.commodity} options={this._getCommodityOptions()} disabled={!this.state.selectCommodity} onChange={this._handleSelection.bind(this, 'commodity')} />
                </div>
                <div className="select-container">
                    <ReactSelect ref="varietySelect" name="varietySelect" value={this.state.variety} options={this._getVarietyOptions()} disabled={!this.state.selectVariety} onChange={this._handleSelection.bind(this, 'variety')} />
                </div>
                <div className="button-container">
                    <RaisedButton style={{ margin: 3 }} label="Clear" secondary={true} onTouchTap={this._handleClear} />
                    <RaisedButton style={{ margin: 3 }} label="Add" primary={true} disabled={!this.state.submitable || this.props.loading} backgroundColor={this.state.addColor} onTouchTap={this._handleAdd} />
                </div>
            </div>
        );
    }
});