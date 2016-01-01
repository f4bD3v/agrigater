const { DropDownMenu, FlatButton } = mui;

QualityControls = React.createClass({

    componentDidMount: function() {
        var self = this;
        var sliders = document.getElementsByClassName("quality-slider");
        console.log(sliders);
        var starts = [0, 100, 100];
        var connects = ["upper", "lower", "lower"];
        //var thresholdTypes = ["coverage", "missingTonnages", "missingPrices"];
        for(var i=0; i < sliders.length; i++) {
            console.log(i);
            console.log(sliders[i]);
            var options = {
                start: starts[i],
                connect: connects[i],
                orientation: "vertical",
                direction: "rtl",
                step: 10,
                range: {
                    'min': 0,
                    'max': 100
                },
                format: wNumb({
                    decimals: 0
                })
            };

            // TODO: add pips to last slider
            if (i==2) {
                options.pips = { // Show a scale with the slider
                    mode: 'steps',
                    density: 1
                } 
            }
            noUiSlider.create(sliders[i], options);
            // use ids here to call method passed by props, one method handles all?
        }

        sliders[0].noUiSlider.on('update', function(values) {
            self.props.changeThreshold(parseInt(values[0], 10), "coverage");
        });

        Opentip.styles.agrigater = {
          // Make it look like the alert style. If you omit this, it will default to "standard"
          className: 'agrigater',
          shadow: false,
          //shadowBlur: 0, 
          //shadowOffset: [1,1],
          //shadowColor: "rgba(0,0,0,.5)", 
          background: "#fff",
          borderColor: "#f0f0f0",
          closeButtonCrossColor: "#444",
          showOn: null
          // Tells the tooltip to be fixed and be attached to the trigger, which is the default target
        };

        Opentip.lastZIndex = 9999;
        
        var coverageTip = new Opentip($("#coverage"), "Sets minimal required percentage of time series coverage", "Coverage Threshold", { delay: 1, style: "agrigater" });
        $('#coverage').hover(function() {
            coverageTip.prepareToShow();
        }, function() {
            coverageTip.hide();
        });

        console.log(sliders[1]);
        sliders[1].noUiSlider.on('update', function(values) {
            self.props.changeThreshold(parseInt(values[0], 10), "missingPrices");
        });

        var priceTip = new Opentip($("#price-missing"), "Sets maximum allowed percentage of missing <b>price</b> entries", "Missing Prices Limit", { delay: 1, style: "agrigater" });
        $('#price-missing').hover(function() {
            priceTip.prepareToShow();
        }, function() {
            priceTip.hide();
        });

        console.log(sliders[2]);
        sliders[2].noUiSlider.on('update', function(values) {
            self.props.changeThreshold(parseInt(values[0], 10), "missingTonnages");
        });

        var tonnageTip = new Opentip($("#tonnage-missing"), "Sets maximum allowed percentage of missing <b>tonnage</b> entries", "Missing Tonnages Limit", { delay: 1, style: "agrigater" });
        $('#tonnage-missing').hover(function() {
            tonnageTip.prepareToShow();
        }, function() {
            tonnageTip.hide();
        });

    },

    _resetControls: function() {
        var sliders = document.getElementsByClassName("quality-slider");
        sliders[0].noUiSlider.set(0);
        sliders[1].noUiSlider.set(100);
        sliders[2].noUiSlider.set(100);
    },

    render: function() {
        /*
        let menuItems = [
            { text: 'Record Time Coverage' },
            { text: 'Total Records' },
            { text: 'Missing Data in Records (Price)' },
            { text: 'Missing Data in Records (Tonnage)' },
        ];
        <DropDownMenu className="quality-dropdown" menuItems={menuItems} />
        */
        // if hidden: show <FlatButton label="Show Controls" />
        return (
            <div className="quality-controls-container">    
                <div className="meta-controls">
                    <FlatButton label="Reset" onTouchTap={this._resetControls} />
                    <FlatButton label="Hide"/>
                </div>
                <div className="quality-controls">
                    <div className="quality-slider" id="coverage" />
                    <div className="quality-slider" id="price-missing" />
                    <div className="quality-slider" id="tonnage-missing" />
                </div>
            </div>
        );
    }

});