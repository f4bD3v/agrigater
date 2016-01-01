/* TODO: 
 - additional data for tooltip like #pointsContributing: http://stackoverflow.com/questions/8514457/set-additional-data-to-highcharts-series,
 http://stackoverflow.com/questions/13559006/adding-new-data-to-highchart-tooltip, http://stackoverflow.com/questions/11294326/highcharts-pass-multiple-values-to-tooltip
 tooltip: {
    shared: True,
    formatter: function() {
        var result = '<b>' + Highcharts.dateFormat('%A, %b %e, %Y', this.x) + '</b>';
        $.each(this.points, function(i, datum) {
            console.log(i); 
            console.log(datum);
        });
    }
 }
*/

const { RefreshIndicator } = mui;

SeriesChart = React.createClass({
    // that of children is invoked before parents
    getInitialState: function() {
        return {
            firstSeries: true,
            seriesOptions: {
                connectNulls: true, // true if dataGrouping activated by default
                gapSize: 0,
            }
        }
    },

    componentDidMount: function() {
        this.series = [];
        this.usedColors = [];
    },

    componentDidUpdate: function() {
        // make it update when props are received and move renderChart here
    },

    componentWillReceiveProps: function(nextProps) {
        // set state here does not trigger additional render
    },

    shouldComponentUpdate: function(nextProps, nextState) {
        // same as with map: we do not want this component to re-render under any circumstances => componentWillUpdate & componentDidUpdate will not be called
        if (this.state.firstSeries && !nextState.firstSeries) {
            return false;
        }
        if (this.state.firstSeries) {
            // load initData
            this._renderChart(nextProps.data);
            this.setState({ firstSeries: false });
            return false;
        }
        console.log(this.props.color);
        console.log(nextProps.color);
        console.log(this.props.loading);
        console.log(nextProps.loading);
        if (this.props.loading !== nextProps.loading) {
            this._addToChart(nextProps.data); 
        }
        return false;
        // or let DOM re-render and create new chart in componentDidUpdate?
    },

    // TODO: how to avoid re-rendering upon this state change
    _getRandomColor: function() {
        var color = randomColor();
        if (this.usedColors.indexOf(color) > -1) {
            return this._getRandomColor();
        } else {
            this.usedColors.push(color);
            return color;
        }       
    },

    _createSeriesObj: function(commodity, unitName, id, dataType, data, color) {
        // actually should be using nextProps
        //var id = [this.props.adminUnit, this.props.unitName, this.props.commodity, dataType].join('.');
        console.log(id);
        var seriesType = (dataType==='price') ? 'spline' : 'column'; // trying out spline instead of line
        var name = commodity+' ('+unitName+')';//this.props.commodity+' ('+this.props.unitName+')';
        var seriesObj = {
            id: id+'.'+dataType,
            type: seriesType,
            name: (dataType==='price') ? (name + ': Price (Rs/kg)') : (name + ': Tonnage'),
            data: data,
            color: color,
            dataGrouping: {
                approximation: (dataType==='price') ? 'average' : 'sum',
            },
            // TODO: needs to include current configuration
            yAxis: (dataType==='price') ? 0 : 1
        }; 
        for (var attrName in this.state.seriesOptions) { seriesObj[attrName] = this.state.seriesOptions[attrName]; } 
        return seriesObj;
    },

    _createRecord: function(dataDoc, dataType) {
        var record = {
            x: dataDoc.date,
            y: dataDoc[dataType],
            contrib: dataDoc.contrib
        };
        return record;
    },

    _docReplaceNan: function(doc) {
        // doc[dataType] = nantoNull(doc[dataType])
        // TODO: this is a quickfix for conversion issue in data collection
        // if (doc["price"] > 100)
        //    doc["price"] = doc["price"]/100;
        doc["price"] = this._nanToNull(doc["price"]);
        doc["tonnage"] = this._nanToNull(doc["commodityTonnage"]);
        return doc;
    },

    _nanToNull: function(value) {
        if (isNaN(value)) {
            value = null;
        }
        return value;
    },

    _retrieveColorByID: function(adminUnit, id) {
        console.log(this.props.selected);
        var selectedItems = this.props.selected.filter(function(obj) { return obj.adminUnit === adminUnit; })[0].selected;
        console.log(selectedItems);
        console.log(id);
        var item = selectedItems.filter(function(obj) { return obj.id === id; })[0];
        console.log(item);
        return item.color;
    },
    // TODO: this.series.name undefined

    // make a loop of this if necessary
    _processData: function(dataSeries) {
        // var dataDocsArray = this.props.data;
        // var seriesData = [];
        // for (i in dataDocsArray) {
        // var dataDocs = this.props.data; //dataDocsArray[i];
        var series = [];
        console.log(dataSeries);
        for (i in dataSeries) {
           var data = dataSeries[i];
            //retrieveColorBID
            var color = this.props.color;//this._getRandomColor();
            // TODO: check if this works
            console.log(data.id);
            var color = this._retrieveColorByID(data.adminUnit, data.id);
            var lineData = [];
            var columnData = [];
            var commodityName = data.commodity;
            // TODO: have object here with color and data.
            //dataDocs[0].name
            //dataDocs[0].adminUnit
            var dataDocs = data.data;
            console.log(data.data);
            for (d in dataDocs) {
                var dataDoc = this._docReplaceNan(dataDocs[d]);
                i
                //if (!commodityName) {
                //    commodityName = dataDoc.commodity;
                //}
                var lineRecord = this._createRecord(dataDoc, 'price');
                lineData.push(lineRecord);
                var columnRecord = this._createRecord(dataDoc, 'tonnage');
                columnData.push(columnRecord);
            }
            var line = this._createSeriesObj(commodityName, data.unitName, data.id, 'price', lineData, color);
            //this.series.push(line);
            series.push(line);
            idElems = data.id.split('.');
            console.log(idElems);
            console.log(idElems[idElems.length-1]);
            if (idElems[idElems.length-1] === 'avg') {
                var column = this._createSeriesObj(commodityName, data.unitName, data.id, 'tonnage', columnData, color);
                //this.series.push(column);
                series.push(column);
            }
        }
        console.log(series);
        // _processData what to return
        return series;
        /*
        if (dataSeries.length > 2) {
            return series;
        } else {
            return series[:2];
        }*/
    },

    _removeFromChart: function(id) {
        console.log(id);
        this.chart.get(id).remove();
        /*
        var series = this.series;
        console.log(id);
        this.series = series.filter(function(obj) { 
            console.log(obj.id);
            return obj.id !== id; 
        });
        console.log(this.series);
        */
        //this._setNavigator();
    },

    _addToChart: function(dataDocs) {
        var objs = this._processData(dataDocs);
        console.log('here we are in _addToChart');
        for(var index in objs)
            this.chart.addSeries(objs[index]);
        //this.chart.addSeries(objs[1]);
        //this._setNavigator(); 
    },

    // TODO: data grouping smoothed true??


    _setNavigator: function() {
        var nav = this.chart.get('navigator');
        var navSeries = {
            name: 'nav',
            data: []
        };
        // TODO: why do series all have empty data fields???
        // !IMPORTANT!: http://stackoverflow.com/questions/16485206/can-not-get-series-data-in-highcharts-highstock
        // OR: save longest data in component variable during _processData() and then set last set to navigator here, but then what to do when removing???
        this.series.forEach(function(series) {
            // take only the 
            if (series.data.length > navSeries.data.length) {
                console.log(series.data);
                navSeries.data = series.data;
            }
        });
        console.log(navSeries.data.length);
        console.log(navSeries.data);
        console.log(this.chart.series);
        //this.chart.series[this.series.length]
        nav.setData(navSeries.data);
        //nav.setData(navSeries.data);
        // better: set baseSeries to series id
        // set extremes allows dynamically setting a range with two datesx`
        //this.chart.xAxis[0].setExtremes(); 
    },

    _setGrouping: function(bool) {
        var gapSize = bool ? 0 : 1;
        var seriesOptions = {
            dataGrouping: {
                enabled: bool
            },
            connectNulls: bool,
            gapSize: gapSize
        };
        this.chart.series.forEach(function(series) {
            series.update(seriesOptions);
        });
        this.setState({ seriesOptions: seriesOptions });
    },

    _setMarkers: function(bool) {
        // NOTE: cannot update plotOptions dynamically, only series objects
        this.chart.series.forEach(function(series) {
            series.update({ 
                marker: {
                    enabled: bool,
                    width: 2 
                }
            });
        });
    },

    _renderChart: function(dataDocs) {
        //var series1=[5,6, null, null, 7,4,3,2,1,0,5,5.5,4,3.5,4,5.5,6];
        //var series2=[5.5,5,4.5,4,4.5,5,6,6.5,7, null, null, 10,10.5,9,8,7.5,7.5];
        //var column1 = [30,40,50,55,45,60,70,80,100,null, null, 60,40,30,40,50,15];
        //var column2 = [60, 40, null, null, 30, 45, 55, 60, 70, 75, 78, 81, 85, 90, 92, null, 80]
        //var dates = ['2005-01-01', '2005-01-02', '2005-01-03', '2005-01-04', '2005-01-05', '2005-01-06', '2005-01-07', '2005-01-08', '2005-01-09', '2005-01-10', '2005-01-11', '2005-01-12', '2005-01-13', '2005-01-14', '2005-01-15'];
        var config = {
            chart: {
                renderTo: 'seriesChart',
            },
            rangeSelector: {
                selected: 4
            },
            navigator: {
               enabled: true,
               series: {
                id: 'navigator' 
               } 
            },
            title: {
                text: '' // no waste of space
            },
            xAxis: {
                ordinal: false 
            },
            yAxis: [{
                title: {
                    text: 'price'
                },
                height: '60%',
                lineWidth: 1
            }, {
                title: {
                    text: 'tonnage'
                },
                top: '65%',
                height: '35%',
                offset: 0,
                lineWidth: 1
            }],
            plotOptions: {
                column: {
                    stacking: 'normal'
                },
                series: {
                    connectNulls: true, // true if dataGrouping activated by default
                    gapSize: 0,
                    turboThreshold: 0,
                    marker: {
                        enabled: true,
                        radius: 2 // will show above a value of 1
                    },
                    dataGrouping: {
                        enabled: true,
                        // TODO: need custom aggregation function, that does not break sourced markets (+ add varieties)
                        // averaging for line, summing for column
                        approximation: 'average',
                        smoothed: true, // account for left shift of data groups (instead of plotting at first point of group)
                        forced: true, // forces grouping of randomly spaced points even when there is still space in the viewport (When data grouping is forced, it runs no matter how small the intervals are; Sometimes dataGrouping won't be used (if not necessary, why group 10points into 3 points) - in that case setup forced: true
                        units: [
                            [
                                'day',
                                [1]
                            ],
                            [
                                'week',
                                [1]
                            ],
                            [
                                'month',
                                [1, 3]
                            ], [
                                'year',
                                null
                            ]
                        ]
                    },
                    //shadow : true
                }
            },
            series: this._processData(dataDocs),
            tooltip: {
                shared: true,
                formatter: function() {
                    var priceFormat = wNumb({
                        decimals: 2,
                    });
                    var tonnageFormat = wNumb({
                        decimals: 0,
                        thousand: ','
                    });
                    var result = '<b>' + Highcharts.dateFormat('%A, %b %e, %Y', this.x) + '</b>';
                    console.log(result);
                    var rows = [result];
                    $.each(this.points, function(i, datum) {
                        var p = datum.y;
                        var markets = datum.point.contrib > 1 ? 'markets' : 'market';
                        var aggregateNotice = datum.point.contrib ? '(aggregated from '+datum.point.contrib+' '+markets+')' : '';
                        var unit = $.trim(this.series.name.split(':')[1]);
                        if (unit === 'Tonnage') {
                            unit = 'Tonnes'
                            p = tonnageFormat.to(datum.y);
                        } else {
                            unit = 'Rupees per kg'
                            p = priceFormat.to(datum.y);
                        }
                        var row = '<br/><b style="color: '+datum.color+'; font-weight: bold;">'+p+'</b> '+[unit, aggregateNotice].join(' ');
                        rows.push(row);
                        console.log(i);
                        console.log(this.series.name);
                        console.log(datum);
                        console.log(datum.point.contrib);
                        console.log(datum.y);
                    });
                    return rows.join('');
                }
            }
        }
        //var priceChart = this.refs.priceChart;
        //var tonnageChart = this.refs.tonnageChart;
        //$(priceChart).StockChart(config);
        //var chartContainer = this.refs.series;
        //this.chart = $(chartContainer).highcharts('StockChart', config);
        this.chart = new Highcharts.StockChart(config);
        //this._setNavigator();
    },
    render: function() {
        // compute left and top
        return (
            <div id="seriesChart" ref="series" />
        );
    }
});