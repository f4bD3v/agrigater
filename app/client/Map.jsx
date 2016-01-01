/* NOTE: looked into datamaps.io, not useful given the requirements of this project
/* TODO: selecting features http://stackoverflow.com/questions/30833825/only-return-selected-geojson-elements-in-leaflet */
// NOTE: Color pallette ['#fc8d59','#ffffbf','#91cf60'], http://colorbrewer2.org/?type=diverging&scheme=RdYlGn&n=3
// Seven rarely used leaflet features: http://blog.webkid.io/rarely-used-leaflet-features/
/* NOTE: for d3 integration: 
  - this: https://gist.github.com/ZJONSSON/3087431
  - and this: http://bl.ocks.org/monfera/11100987
  - more complicated way of doing it derived from bostocks tutorial: https://gist.github.com/d3noob/9211665
  THIS: https://github.com/teralytics/Leaflet.D3SvgOverlay
*/
Map = React.createClass({
  // invoked once on the client after rendering occurs!
  // --> meaning div for which to load the map will already be there
  getDefaultProps: function() {
    return {
      layers: []
    };
  },

  getInitialState: function() {
    return {
      transition: false,
      layers: false,
      zoomed: false,
      districts: false,
      horzTransition: false,
      bubbles: false,
      zoomLevel: 5,
      zoomedLevel: -1
    };
  },

  componentDidMount: function() {
    var self = this;
    // TODO: there is no need for self in component methods (this as methods are bound to the component instance (this call in method refers to component)
    //self.L.Icon.Default.imagePath = '/images';
    this.L = L;
    var southWest = L.latLng(-10, 60),
    northEast = L.latLng(50, 100),
    bounds = L.latLngBounds(southWest, northEast);
    this.initViewCoords = new this.L.LatLng(21.146633, 79.088860)
    this.initZoomLevel = 5;
    this.map = this.L.map('map', {
      maxBounds: bounds,
      doubleClickZoom: false,
      minZoom: 5,
      maxZoom: 12,
      zoomControl: false
    }).setView(this.initViewCoords, this.initZoomLevel);
    //// TODO: disable dragging at zoom level 5
    this.map.dragging.disable();
    this.map.scrollWheelZoom.disable();
    this.initBounds = bounds;

    //this.zoomHome = L.Control.zoomHome();
    //this.zoomHome.addTo(this.map);

    var tiles = this.L.tileLayer.provider('Esri.WorldPhysical');
    // Alternatives: Esri.(WorldStreetMap|WorldTopoMap|WorldImagery)
    tiles.addTo(this.map);
    this.baseLayerObj = {
      "Tiles" : tiles
    };

    var stateCenters = Object.create(null);
    this.stateCenters = stateCenters;

    var districtCenters = Object.create(null);
    this.districtCenters = districtCenters;

    //var unitCenters = Object.create(null);
    //this.unitCenters = unitCenters;
    /*
    //////// ADDING D3 ///////
    // Initialize the SVG layer
    this.map._initPathRoot()    

    // We simply pick up the SVG from the map object
    var svg = d3.select("#map").select("svg");
    this.g = svg.append("g"); 
    //////////////////////////
    */
    this.info = this._createInfoControl();
    this.info.addTo(this.map);

    this.overlays = this.L.layerGroup().addTo(this.map);
    this.overlayObj = {};
    //this.map.setMaxBounds(bounds);
    //this.map.setMinZoom(5);
    this.map.fitBounds(bounds);
    this.map.on("zoomend", function() {
      if (self.map.getZoom() !== self.state.zoomLevel) {
        self.setState({
          zoomLevel: self.map.getZoom()
        });
      }
    });
  },

  // Invoked whenever there is a prop change
  // Called BEFORE render
  componentWillReceiveProps: function(nextProps){
  // Not called for the initial render
  // Previous props can be accessed by this.props
  // Calling setState here does not trigger an an additional re-render

  /// CHANGE CURSOR FOR COMPARISON MODE
  if (nextProps.comparison && !this.props.comparison) {
    $('.leaflet-clickable').css('cursor', 'default');
  } else if (this.props.comparison && !nextProps.comparison) {
    $('.leaflet-clickable').css('cursor', '');
  }

  // TODO: when I call setState do I want an immediate additional re-render like in shouldComponentUpdate?
  },

  // Determines if the render method should run in the subsequent step
  // Called BEFORE a render
  // Not called for the initial render
  shouldComponentUpdate: function(nextProps, nextState) {
    // If you want the render method to execute in the next step
    // return true, else return false

    /// ADDING STATE/MARKET LAYERS /// 
    console.log(nextProps.districtLayer);
    if (!nextState.layers) {
      this._addStateAndMarketLayers(nextProps.layers);
      this.setState({
        layers: true  
      });
    }
    //////////////////////////////////

    /// ADDING DISTRICTS LAYER ///
    if (!nextState.districts && nextProps.districtLayer.length > 0) {
      this._addDistricts(nextProps.districtLayer);
      var stateUpdate = {
        districts: true
      };
      if (this.props.visMode === 'trade') {
        console.log('adding Bubbles');
        console.log(nextProps.unitData);
        this._addBubbles(nextProps.unitData);
        stateUpdate.bubbles=true;
      }
      this.setState(stateUpdate);
      return;
    }
    ///////////////////////////////

    /// ADDING/REMOVING MARKETS ///
    if (nextProps.markets) {
      if (!this.props.markets) {
        this.control.addOverlay(this.overlayObj['Markets'], "Markets"); 
        this.map.addLayer(this.overlayObj['Markets']); 
      }
      var nextMarketData = nextProps.marketData;
      if (nextMarketData.length > 0 && (this.props.marketData.length === 0 || this.props.marketData.length !== nextProps.marketData.lenght)) {
        console.log('changing markets displayed');
        this.overlayObj['Markets'].clearLayers();
        var markersToKeep = nextMarketData.map(function(obj) { return [obj.market, obj.district, obj.state].join('.'); })
        console.log(markersToKeep);
        //var clusterGroup = this.overlayObj['Markets'];
        var markersToAdd = this.markers.filter(function(obj) {
          var layer = obj.marker;
          var props = obj.props;
          var district = (props.district==="null") ? '' : props.district;
          var markerId = [props.name, props.district, props.state].join('.');
          return (markersToKeep.indexOf(markerId) > 0);
        });
        this.overlayObj['Markets'].addLayers(markersToAdd.map(function (obj) { return obj.marker; }));
      }
    } else {
      if (this.props.markets) {
        this.control.removeLayer(this.overlayObj['Markets']);
        this.map.removeLayer(this.overlayObj['Markets']);
      }
    }
    ///////////////////////

    /// HIGHLIGHTING STATES ///
    // NOTE: states are always drawn => unitData can either be districts or states, can check with districtLayer.length > 0?
    // does not depend on commodity, but data sent
    var nextUnitData = nextProps.unitData;
    console.log('nextUnitData');
    console.log(nextUnitData);
    console.log(this.state.districts);
    console.log(nextState.districts);
    console.log(this.state.zoomed);
    console.log(nextState.zoomed);
    // NOTE: for first loading
    if (!this.state.districts && nextState.districts) {
      console.log('hello districts!');
      this._resetStyle('Districts');
      console.log(this.props.unitData);
      this._highlight(this.props.unitData, 'Districts');
      // TODO: highlight districts, probably do not need to reset.. DO NEED TO RESET STYLE, because threshold controls may reduce districts to be highlighted
      // update: reset styles for all districts => call highlight on districts returned with unitData
      // what would be a good highlight for districts, not red; darken beige a bit? use tinycolor method?
    }
    // this.props.commodity useful???
    // TODO: set a unitDataChange variable to make it possible to update at later stage?
    if (nextUnitData.length > 0 && (this.props.unitData.length === 0 || this.props.unitData.length !== nextUnitData.length)) {
      console.log('is it happening?');
      // NOTE: for threshold changes
      if (nextState.districts) {
        console.log('hello districts!');
        this._resetStyle('Districts');
        this._highlight(nextUnitData, 'Districts');
        // TODO: highlight districts, probably do not need to reset.. DO NEED TO RESET STYLE, because threshold controls may reduce districts to be highlighted
        // update: reset styles for all districts => call highlight on districts returned with unitData
        // what would be a good highlight for districts, not red; darken beige a bit? use tinycolor method?
      } else if(!nextState.zoomed || !this.state.zoomed) {
        // TODO: put layer deactivation code here
        //this._resetStyle('States');
        //this._disableStates(nextProps.unitName);
        console.log('resetting');
        // TODO: is this causing problem's with mouseout after zoomout
        this._resetStyle('States');
        console.log(nextUnitData);
        /*
        var data = _.map(nextUnitData, function(d) {
          return d.recordsTotal;
        });
        console.log(data);
        var mean = math.mean(data);
        var std = math.std(data);
        // markets have coordinates with lat and lng coordinates, do I need reactive join?
        var heatMapData = {
          min: math.min(data),
          max: math.max(data),
          data: [{lat: .., lng: .., count: ..}, ..]
        };
        console.log(math.min(data));
        console.log(mean-2*std);
        console.log(mean-std);
        console.log(mean);
        console.log(mean+std);
        console.log(mean+2*std);
        console.log(math.max(data));
        // blue highlight
        */
        this._highlight(nextUnitData, 'States');
      }
    }
    ///////////////////////////

    /// DRAWING DATA ///
    if (this.props.visMode === 'trade' && nextProps.visMode !== 'trade') {
      // NOTE: remove bubbles overlay when vis mode changes, BUT ALSO WHEN DATA CHANGES...
      this.control.removeLayer(this.bubblesOverlay);
      this.map.removeLayer(this.bubblesOverlay);
      this.map.removeLayer(this.bubblesLegend);
      this.setState({
        bubbles: false 
      });
    }
    if (nextProps.visMode === 'trade' && nextUnitData.length > 0 && this.props.unitData.length !== nextUnitData.length) {
      if (this.state.bubbles) { // && nextState.bubbles) {
        this.map.removeLayer(this.bubblesOverlay);
        this.control.removeLayer(this.bubblesOverlay);
        this.map.removeLayer(this.bubblesLegend);
      }
      // TODO: fix bubble size and change legend labels dynamically
      if (this.props.adminUnit === 'country' || nextState.districts) {
        console.log("I SHOULDN'T BE HERE!");
        this._addBubbles(nextUnitData);
        this.setState({
          bubbles: true
        });
      }
    }
    //////////////////////

    if (nextState.zoomingIn && nextState.zoomLevel > this.state.zoomLevel && nextState.zoomLevel > this.initZoomLevel) {
      console.log('changing zoomedLevel');
      this.setState({
        zoomingIn: false,
        zoomedLevel: nextState.zoomLevel,
        zoomed: true
      });
      // TODO: how do I know where I've zoomed to?
    }

    /// if zoomed and not zoomed previously: gray out all layers but selected state
    if (nextState.zoomed && !this.state.zoomed) {
      var self = this;
      this.overlayObj["States"].eachLayer(function(layer) {
        if (layer.feature.properties.name !== self.props.unitName) {
          layer._disabled = true; 
          layer.setStyle(self._returnStyle("inactive", false));
        }
      });
    }

    /// REMOVING DISTRICTS LAYER ON ZOOMOUT
    console.log(this.state.zoomedLevel);
    console.log(nextState.zoomLevel);
    if (nextState.zoomLevel < this.state.zoomedLevel) {
      var self = this;
      this._removeDistricts();
      this.map.fitBounds(this.initBounds);
      this.map.scrollWheelZoom.disable();

      this.overlayObj["States"].eachLayer(function(layer) {
        layer._disabled = false; 
        layer.setStyle(self._returnStyle(layer._lastStyle, false));
      });

      this.setState({
        zoomedLevel: -1,
        zoomed: false,
        districts: false
      });
      this.props.onReset();
      //this.map.setView(this.initViewCoords, this.initZoomLevel);
    }
    return false;
  },

  _createInfoControl: function() {
    var info = L.control();
    info.onAdd = function (map) {
        this._div = L.DomUtil.create('div', 'info'); // create a div with a class "info"
        this.reset();
        //this.update();
        return this._div;
    };

    // pre-format the html content for the control
    info.update = function (content) {
        this._div.innerHTML = content;
    };

    info.reset = function() {
      this._div.innerHTML =  '<h1>Welcome to Agrigater!</h1>'
        +'<p>a visualization of open Indian agricultural data gleaned from government websites</p>';
    };
    return info;
  },

  _addBubbles: function(aggregates) {
    var self = this;
    // use adminUnit or districtsLayer?
    aggregates.forEach(function(d) {
      if (self.props.adminUnit === 'state') {
        d.latlng = self.districtCenters[d.name];
      } else {
        d.latlng = self.stateCenters[d.name];
      }
    });
    level = "States";
    if (self.props.adminUnit === 'state')
      level = "Districts";
    var aggregates = aggregates.filter(function(d){ return d.latlng !== undefined });
    var radius = d3.scale.sqrt()
      .domain([0, 5e6]) // 50000
      .range([0, 66]); // 15
    this.bubblesOverlay = L.d3SvgOverlay(function(sel, proj) {
    //.attr("fill", "#ff0000")
    // TODO: find way to access bubbles from states layer mouseover and fire mouseover event (set _mouseOver flag on states to prevent infinite loop)
    var feature = sel.selectAll("circle")
      .data(aggregates
      .sort(function(a, b) { return b.aggrTonnage - a.aggrTonnage; }))
      .enter().append("circle").attr("class", "bubble")
      .attr("name", function(d) { return d.name })
      .attr("cx", function(d) { return proj.latLngToLayerPoint(d.latlng).x})
      .attr("cy", function(d) { return proj.latLngToLayerPoint(d.latlng).y})
      .attr("r", function(d) { return radius(d.aggrTonnage)})
      .on("mouseover", function(d) {
        d3.select(this)
          .classed("active", true) 

        self.overlayObj[level].eachLayer(function(layer) {
          if (layer.feature.properties.name === d.name) {
            if (!layer._mousedOver)
              layer.fireEvent('mouseover');
          }
        });
        var tonnageFormat = wNumb({
          thousand: ',',
          decimals: 0
        });
        var content = '<h2>'+d.name+'</h2>'
        +'<b>'+tonnageFormat.to(d.aggrTonnage)+'</b> tonnes of <b>'+d.commodity+'</b><br/>'
        +'<p>recorded trade between<br/><i>'+self._formatDate(d.startDate)+'</i> and<br/><i>'+self._formatDate(d.endDate)+'</i>';
        self.info.update(content);
      })
      .on("mouseout", function(d) { 
        self.overlayObj[level].eachLayer(function(layer) {
          if (layer.feature.properties.name === d.name) {
            console.log(layer._mousedOver);
            if (layer._mousedOver)
              layer.fireEvent('mouseout');
          }
        });
        d3.select(this)
          .classed("active", false)
        self.info.reset()
      });
      // put mouseover here: leaflet zoombutton click event http://stackoverflow.com/questions/19661090/leaflet-d3-no-mouseover-on-safari
    });
    this.bubblesOverlay.addTo(this.map);
    this.control.addOverlay(this.bubblesOverlay, "Bubbles"); 
    /*self.overlayObj["States"].eachLayer(function(layer) {
      if (layer.feature.properties.name === "Madhya Pradesh") {
        layer.fireEvent('mouseout');
    });
    stuff.fireEvent('mouseover')
    */
    //this.bubblesOverlay.eachLayer(function(layer) {
    //  layer.fireEvent('mouseover');
    //});

    this.bubblesLegend = L.d3SvgOverlay(function(sel, proj) {
      var height = $(window).height();
      var width = $(window).width();
      var legend = sel.append('g')  
        .attr("class", "legend")
        //.attr("transform", "translate(" + 200 + "," + (height - 150) + ")")
        .selectAll("g")
          .data([100000, 1e6, 5e6])
          .enter().append("g")

      legend.append("circle")
        .attr("cy", function(d) { return -radius(d); })
        .attr("r", radius);

       legend.append("text")
        .attr("y", function(d) { return -2 * radius(d); })
        .attr("dy", "1.3em")
        .text(d3.format(".1s"));
    });
    this.bubblesLegend.addTo(this.map);
  },

  // Create a string representation of the date.
  _formatDate: function(date) {
    // add a monthly average as comparison?
    var weekdays = [
      "Sunday", "Monday", "Tuesday",
      "Wednesday", "Thursday", "Friday",
      "Saturday"
    ],
    months = [
      "January", "February", "March",
      "April", "May", "June", "July",
      "August", "September", "October",
      "November", "December"
    ];
    // Append a suffix to dates.
    // Example: 23 => 23rd, 1 => 1st.
    function nth (d) {
      if(d>3 && d<21) return 'th';
      switch (d % 10) {
            case 1:  return "st";
            case 2:  return "nd";
            case 3:  return "rd";
            default: return "th";
        }
    }
    return weekdays[date.getDay()] + ", " +
        date.getDate() + nth(date.getDate()) + " <b>" +
        months[date.getMonth()] + " " +
        date.getFullYear() + "</b>";
  },

  _disableStates: function(bli, bla) {
    console.log('disabling');
  },

  _highlight: function(toDisplay, level) {
    self = this;
    console.log(toDisplay);
    var displayable = [];
    if (level === 'States')
      displayable = toDisplay.map(function(obj) { return obj.state; });
    else
      displayable = toDisplay.map(function(obj) { return obj.district; });
    console.log(displayable);
    this.overlayObj[level].eachLayer(function(layer) {
      if (displayable.indexOf(layer.feature.properties.name) > -1) {
        layer._disabled = false;
        layer._lastStyle = "highlighted";
        layer.setStyle(self._returnStyle("highlighted", false));
      } else {
        layer._disabled = true;
      }
      layer.on("mouseout", function () {
        console.log('hellooo');
        if(!this._disabled && this._mousedOver) {
          this._mousedOver = false;
          console.log('mousing out');
          console.log(displayable);
          if (self.props.comparison && self.props.selection.indexOf(this.feature.properties.name) > -1) {
            this.setStyle(self._returnStyle("selected", false));
          } else if (displayable.indexOf(this.feature.properties.name) > -1) {
            this.setStyle(self._returnStyle("highlighted", false));
          } else {
            this.setStyle(self._returnStyle(level, false));
          }
          if (self.props.visMode === 'trade') { //&& this.feature.properties.level === 'State') {
            var circle = $("circle[name='"+this.feature.properties.name+"']").get(0);
            // answer in the middle by Fredrik Johanson: http://stackoverflow.com/questions/9063383/how-to-invoke-click-event-programmatically-in-d3
            var ev = document.createEvent("SVGEvents");
            ev.initEvent("mouseout", false, false);
            circle.dispatchEvent(ev);
          }
        } else {
          if (self.props.comparison) 
            $('.leaflet-clickable').css('cursor', 'default');
          else
            $('.leaflet-clickable').css('cursor', '');
        }
      });
    });
  },

  _resetSelectionStyles: function() {
    var self = this;
    var level = this.state.zoomed ? "Districts" : "States";
    this.overlayObj[level].eachLayer(function(layer) {
      if (self.props.selection.indexOf(layer.feature.properties.name) > -1) {
        layer.setStyle(self._returnStyle(layer._lastStyle, false));
      }
    });
  },

  _resetStyle: function(level) {
    self = this;
    this.overlayObj[level].eachLayer(function(layer) {
      layer._lastStyle=level;
      layer.setStyle(self._returnStyle(level, false));
    });
  },

  _removeDistricts: function() {
    /* TODO: find out how to remove by ID */
    this.map.removeLayer(this.districtsL);
  },

  _addDistricts: function(districts) {
    var self = this;
    this.districtsL = omnivore.topojson.parse(districts[0].topology);
    this.districtsL.setStyle(this._returnStyle("Districts", false));
    //this.overlayObj['Districts'] = districtsL;
    /*
    NOTE: dont need district names, can exclude from click events with _disabled flag
    this.districtNames = [];
    this.districtsL.eachLayer(function(layer) {
      layer._disabled = false;
      self.districtNames.push(layer.feature.properties.name);
    })
    */
    this.districtsL.eachLayer(this._eachLayer);
    this.districtsL.addTo(this.overlays);
    this.overlayObj['Districts'] = this.districtsL;
    this.districtNames = [];
    this.overlayObj['Districts'].eachLayer(function(layer) {
      layer._lastStyle = "Districts";
      layer._disabled = false;
      var props = layer.feature.properties;
      var bounds = layer.getBounds();
      var latlng = bounds.getCenter();
      var districtName = props.name;
      console.log(districtName);
      console.log(latlng);
      self.districtNames.push(districtName);
      self.districtCenters[districtName] = latlng;
    });
    /*
    var control = this.L.control.layers(this.baseLayerObj, this.overlayObj, {
      collapsed: false,
      autoZIndex: true
    });
    control.addTo(this.map)
    this.L.control.scale().addTo(this.map)*/
  },

  _returnStyle: function(layerName, mouseover) {
    if (layerName === "inactive") {
     return {
      color: "#444",
      fillColor: "#acacac",
      weight: 2,
      opacity: .5,
      fillOpacity: .8
     };
    }
    if (layerName === "flash") {
      return {
       color: "#444", //"#c0392b",
       weight: 2,
       opacity: 1,
       fillColor: "#ffffbf",
       fillOpacity: 1
      };
    }
    if (layerName === "selected") {
      return {
        fillColor: "#00bcd4",
        color: "#444",
        weight: 2,
        opacity: 1,
        fillOpacity: .5
      };
    }
    if (layerName === "highlighted") {
      return {
        fillColor: "#c0392b", // "#7f171f", //#00bcd4
        color: "#444", //#fff
        weight: 1,
        opacity: 1,
        fillOpacity: .6
      };
    }
    if (layerName === "States") {
      //var fill = mouseover ? "#ff6600" : "#fb7d05";
      var weight = mouseover ? 3 : 1;
      var fillOpacity = mouseover ? 1 : .5;
      var opacity = mouseover ? 1 : .7;
      return {
        color: "#444", //"#ff6600",
        weight: weight,
        opacity: opacity,
        fillOpacity: fillOpacity,
        fillColor: "#fc8d59" //ffa500
      };  
    } else if (layerName === "Districts") {
      var fill = mouseover ? "#fff" : "#ffffbf";
      var weight = mouseover ? 2: 1;
      return {
        color: "#444",
        weight: weight,
        opacity: 1,
        fillOpacity: 1,
        fillColor: fill //ffa500
      };
    } else {
      new Error("no styles for layer "+layerName);
    }
  },

  _addStateAndMarketLayers: function(layers) {
    var self = this;
    statesTopo = this._filterByID(layers, "states")[0];
    //districtsTopo = this._filterByID(layers, "districts")[0];
    marketsGeo = this._filterByID(layers, "markets")[0];

    var statesL = omnivore.topojson.parse(statesTopo);
    statesL.setStyle(this._returnStyle("States", false));
    statesL.addTo(this.overlays);
    this.overlayObj['States'] = statesL;
    statesL.eachLayer(this._eachLayer);
    this.stateNames = [];
    statesL.eachLayer(function(layer) { 
      layer._lastStyle="States";
      layer._disabled = false;
      var props = layer.feature.properties;
      var bounds = layer.getBounds();
      var latlng = bounds.getCenter();
      var stateName = props.name;
      self.stateNames.push(stateName);
      self.stateCenters[stateName] = latlng;
    });


    /*var districtsL = omnivore.topojson.parse(districtsTopo);
    districtsL.setStyle(this._returnStyle("districts", false));
    districtsL.addTo(this.overlays);
    this.overlayObj['Districts'] = districtsL;
    districtsL.eachLayer(this._eachLayer);*/

    var clusterOptions = { 
      showCoverageOnHover: true,
      spiderifyOnMaxZoom: false,
      zoomToBoundsOnClick: false, // true by default
      removeOutsideVisibleBounds: true, // by default
      maxClusterRadius: 250, // you can increase this, try 250px
      addRegionToolTips: true,
      singleMarkerNode: true,
      disableClusteringAtZoom: 10,
      chunkedLoading: true
    };
    this.completeClusterGroup = new L.MarkerClusterGroup(clusterOptions);
    var marketMarker = this.L.AwesomeMarkers.icon({
      prefix: 'glyphicon',
      icon: 'grain',
      //icon: 'shopping-cart',
      //prefix: 'fa', 
      markerColor: 'green'  // '#02b300'fillColor: '#91cf60'
    });
    self.markers = []; //new self.L.LayerGroup();
    var marketsL = this.L.geoJson(marketsGeo, {
      //style: markerStyle,
      pointToLayer: function(feature, latlng) {
        var marker = new L.marker(latlng, { icon: marketMarker, regions: [feature.properties.district, feature.properties.state] });
        self.markers.push({marker: marker, props: feature.properties});
        return marker;
      },
      onEachFeature: function(feature, layer) {
        layer.on('click', function() { 
          self.props.onClick(feature.properties, false, true);
        });
        layer.bindPopup(self._markerPopup(feature), {closeButton: false, offset: L.point(0, -30)});
        layer.on('mouseover', function() { layer.openPopup(); });
        layer.on('mouseout', function() { layer.closePopup(); });
      }
    });
    this.completeClusterGroup.addLayer(marketsL);
    this.overlayObj['Markets'] = this.completeClusterGroup;
    this.completeClusterGroup.addTo(this.overlays);

    this.control = this.L.control.layers(this.baseLayerObj, this.overlayObj, {
      collapsed: false,
      autoZIndex: true
    });
    this.control.addTo(this.map);
    this.L.control.scale().addTo(this.map)
  },
  /* NOTE: what react does when component receives new props or state
  Call componentWillReceiveProps(nextProps) (if there are new props)
  Call shouldComponentUpdate(nextProps, nextState) (and maybe stop the update)
  Call componentWillUpdate(nextProps, nextState)
  Assign this.props = nextProps and this.state = nextState
  Call render()
  */

  /*** IMPORTANT INFO
  For everyone with a similar problem: What you want to use is the onEachFeature function. The feature represents a geojson object. Using the sample data provided above the id can be accessed through feature.properties.popupContent.
    function onEachFeature(feature, layer) {
        layer.on('click', function (e) {
            alert(feature.properties.popupContent);
            // IF FEATURE DOES NOT WORK, try: e.target.feature..
            //or
            alert(feature.properties.id);
        });
    }
  ***/

  _markerPopup: function(feature) {
    var props = feature.properties;
    var reg = props.regulated ? "Regulated" : "Unregulated";
    var established = (props.established !== null) ? " ("+new Date(props.established).getFullYear()+")" : "";
    var name = "<b>"+props.name+"</b>"+established+"<br/>";
    var district = (props.district !== null || props.district !== "null") ? props.district : "not assigned";
    var address = district + ", " + props.state + "</br>";
    var popup = name + address + "<i>"+reg+"</i>";
    return popup;
  },

  _filterByID: function(array, id) {
    var arr = array.filter(function(obj) {
      return obj["_id"] === id; 
    });
    return arr;
  },

  _flashDistricts: function() {
    var self = this;
    this.overlayObj["Districts"].eachLayer(function(layer) {
      // TODO: ONLY FLASH BORDER, DO NOT CHANGE FILLCOLOR
      layer.setStyle(self._returnStyle("flash", false));
    });
    setTimeout(function() {
      self.overlayObj["Districts"].eachLayer(function(layer) {
        if (self.props.selection.indexOf(layer.feature.properties.name) > -1)
          layer.setStyle(self._returnStyle("selected", false));
        else
          layer.setStyle(self._returnStyle(layer._lastStyle, false));
      });
    }, 500);
  },

  // TODO:
  // how to stop events from being fired: http://stackoverflow.com/questions/28188743/prevent-moveend-event-being-fired-in-leaflet-after-setview
  _eachLayer: function(layer) {
    var self = this;
    var feature = layer.toGeoJSON();
    var level = feature.properties.level + "s"; //.toLowerCase()
    //var marker = this.L.circleMarker(layer.getBounds().getCenter(), { opacity: 0.01 });
    // TODO: populate info control on mouseover, loads statesinfo as props through database at component mount??
    /*layer.bindLabel(feature.properties.name, { 
      noHide: true,
      className: "admin-unit-label",
      direction: "left"
    });*/

    //layer.addLayer(marker);
    layer.on("mouseover", function () {
      // Change the style to the highlighted version
      console.log('mousing over');
      if(!this._disabled && !this._mousedOver) {
        this._mousedOver = true;
        if (!self.props.comparison || self.props.selection.indexOf(this.feature.properties.name) === -1)
          this.setStyle(self._returnStyle(level, true));
        if (self.props.visMode === 'trade') { //&& this.feature.properties.level === 'State') {
          var circle = $("circle[name='"+this.feature.properties.name+"']").get(0);
          // answer in the middle by Fredrik Johanson: http://stackoverflow.com/questions/9063383/how-to-invoke-click-event-programmatically-in-d3
          var ev = document.createEvent("SVGEvents");
          ev.initEvent("mouseover", false, false);
          circle.dispatchEvent(ev);
        }
        /*
        if (self.props.comparison && self.props.selection.indexOf(this.feature.properties.name) > -1) {
          console.log(self.props.comparison);
          console.log(self.props.selection);
          this.setStyle(self._returnStyle("selected", false));
        } else {
          this.setStyle(self._returnStyle(level, true));
        }*/
      } else {
        $('.leaflet-clickable').css('cursor', 'text');
      }
    }); 
    layer.on("mouseout", function () {
      // TODO: Change the style to the highlighted version
      console.log(this._disabled);
      console.log(this._mousedOver);
      if(!this._disabled && this._mousedOver) {
        console.log('wrong mousing out');
        if (!self.props.comparison || self.props.selection.indexOf(this.feature.properties.name) === -1)
          this.setStyle(self._returnStyle(level, false));
        if (self.props.comparison)
          $('.leaflet-clickable').css('cursor', 'default');
        else
          $('.leaflet-clickable').css('cursor', '');
      }
    });
    layer.on("contextmenu", function(e) {
      if(!this._disabled) {
        if (e.preventDefault) e.preventDefault();
        layer.setStyle(self._returnStyle(layer._lastStyle, true));
        // TODO: how to set State or District mouseover?
        self.props.updateSelection(layer.feature.properties.level, layer.feature.properties.name, false);
        // return to original style: borrow code from on mouseout
        // TODO: dblclick on anywhere in the selection opens dialog
      }
    });
    $(layer).fixClick(function(e) {
      console.log('shiftKey');
      console.log(e);
      console.log(e.originalEvent);
      console.log(e.originalEvent.shiftKey);
      console.log(e.originalEvent.altKey);
      console.log(e.originalEvent.ctrlKey);
      var targetLayer = e.target;
      var targetFeature = targetLayer.feature;
      var targetProps = targetFeature.properties;
      if (targetLayer._disabled) {
        //(self.state.zoomed && self.districtNames.indexOf(targetProps.name) === -1) {//self.props.unitName !== targetProps.name) {
        self._flashDistricts();
      } else if (self.props.comparison) {
        if (self.props.selection.indexOf(targetProps.name) > -1 && self.props.selection.length > 1) {
          self.props.onClick(feature.properties, false);
        } else {
          if (self.props.selection.length < 5) {
            targetLayer.setStyle(self._returnStyle("selected", true));
            // TODO: somehow set targetLayer style?
            self.props.updateSelection(targetProps.level, targetProps.name, true);
          } else {
            // TODO: show popup
          }
        }
      } else {
        self.props.onClick(feature.properties, false);
      }
    }, function(e) {
      // => query for district borders within states
      // TODO: if this.state.zoomed disable click events for which targets are not in state
      var targetLayer = e.target;
      var targetFeature = targetLayer.feature;
      var targetProps = targetFeature.properties;
      // check if properties state

      /// DISABLING CLICKS OUTSIDE ZOOMED STATE ///
      // TODO: if name not one of the districtnames
      // TODO: targetLAyer._disabled necessary
      //if (self.state.zoomed && self.districtNames.indexOf(targetProps.name) === -1 || targetLayer._disabled) {//self.props.unitName !== targetProps.name) {
      if (self.props.visMode === 'trade') {
        self.map.removeLayer(self.bubblesOverlay);
        self.control.removeLayer(self.bubblesOverlay);
      }
      if (targetLayer._disabled) {
        self._flashDistricts();
      } else {
        if (!self.props.comparison) {
          self.map.fitBounds(layer.getBounds());
          self.map.scrollWheelZoom.enable();
          self.map.dragging.disable();
          self.map.keyboard.disable();
          //ggconsole.log(self.zoomHome);
          //self.zoomHome._enabled = false;
          //self.zoomHome._updateDisabled();
          //self.zoomHome.disable();
          //console.log(self.L.Control);
          //console.log(self.L.Control.Zoom);
          //console.log(self.zoomHome);
          self.setState({
            zoomingIn: true
          });
          self.props.onClick(feature.properties, true);
        }
      } 
      // doesn't work
      //self.zoomHome.disable();
      //var disableClass = 'leaflet-disabled';
      //console.log(self.L.DomUtil);

      //$('.leaflet-control-zoomhome-out').addClass(disableClass);
      //self.L.Control.Zoom.disable();
      /*if (self.state.zoomed) {
        self._removeDistricts()
      } else {

      }*/
    });
  },

  render: function() {
  	return (
  		<div id='map' />
  	);
  }
});