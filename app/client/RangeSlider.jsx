const { DatePicker } = mui;

RangeSlider = React.createClass({
    getInitialState: function() {
        return {
            rangeStart: new Date(2015, 3, 1),
            rangeEnd: new Date(2015, 9, 1)
        }
    },

    _formatTimestamp: function(timestamp) {
        var date = new Date(+timestamp);
        return this._formatDate(date, true);
    },

    shouldComponentUpdate: function(nextProps, nextState) {
        return false;
    },

    componentDidMount: function() {
        this._initSlider();
    },

    /*
    componentDidUpdate: function() {
        console.log($('#date-slider'));
        $('#date-slider').noUiSlider.destroy();
        this._initSlider();
    },*/

    _initSlider: function() {
        var self = this;

        this.refs.startPicker.setDate(this.props.rangeStart)
        this.refs.endPicker.setDate(this.props.rangeEnd);

        dateSlider = document.getElementById('date-slider');
        noUiSlider.create(dateSlider, {
            // NOTE: create single handle by setting single start value
            start: [
                this.props.rangeStart.getTime(),//this.state.rangeStart.getTime(),   
                this.props.rangeEnd.getTime()//this.state.rangeEnd.getTime()
            ],
            format: wNumb({
                decimals: 0
            }),
            step: 30 * 24 * 60 * 60 * 1000,
            behaviour: 'tap-drag', //drag-fixed
            connect: true,
            // get last recorded date value from database as prop and use it to set max field
            range: {
                'min':  new Date(2005, 1, 1).getTime(),
                'max':  new Date(2015, 9, 1).getTime()
            },
            pips: { // Show a scale with the slider
                mode: 'count',
                // compute number of pips dynamically from data
                values: 2*10,
                density: 1,
                format: {
                    to: this._formatTimestamp
                }
            } 
        });

        dateSlider.noUiSlider.on('update', function(values, handle) {
            var date = new Date(+values[handle]);
            if (handle === 1)
                self.setState({ rangeEnd: date, manual: false });
            else
                self.setState({ rangeStart: date, manual: false });
        });
        // https://refreshless.com/nouislider/events-callbacks/
        // slide event : handle moves while dragging
        // update : handle is released after dragging 
        // change: fires when a handle is released after dragging and slider is moved by tapping
        // ==>Drag change does not fire with set (it does :/)
        /*
        dateSlider.noUiSlider.on('end', function(values, handle) {
            // handle == 0 when first handle value is updated => which is rangeStart
            // hanlde == 1 when 2nd handle value is updated => which corresponds to rangeEnd
            console.log('date slider end event');
            console.log(handle);
            var date = new Date(+values[handle])
            console.log(date)
            console.log(this.state.rangeEnd);
            self.props.onDrag(date, handle);
            (handle === 0) ? self.refs.startPicker.setDate(date) : self.refs.endPicker.setDate(date);
            //dateValues[handle].innerHTML = formatDate(new Date(+values[handle]));
        });*/

        dateSlider.noUiSlider.on('set', function(values, handle) {
            var rangeStart = self.state.rangeStart;
            var rangeEnd = self.state.rangeEnd;
            console.log(values);
            console.log(handle);
            var date = new Date(+values[handle]);
            if (handle === 0)
                rangeStart = date;
            else
                rangeEnd = date;
            self.props.onSet(rangeStart, rangeEnd);
            self.refs.startPicker.setDate(rangeStart);
            self.refs.endPicker.setDate(rangeEnd);
            self.setState({
                rangeStart: rangeStart,
                rangeEnd: rangeEnd,
                manual: true
            });
        });

        /*
        dateSlider.noUiSlider.on('set', function(values, handle) {
            console.log('date slider set event');
            console.log(values);
            console.log(handle);
            var date = new Date(+values[handle])
            self.props.onDrag(date, handle);
            (handle === 0) ? self.refs.startPicker.setDate(date) : self.refs.endPicker.setDate(date);
        });*/

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

        var startTip = new Opentip($(".start"), "", "Time Range Start", { delay: 1, style: "agrigater" });
        $('.start').hover(function() {
            startTip.prepareToShow();
        }, function() {
            startTip.hide();
        });

        var endTip = new Opentip($(".end"), "", "Time Range End", { delay: 1, style: "agrigater" });
        $('.end').hover(function() {
            endTip.prepareToShow();
        }, function() {
            endTip.hide();
        });
    },

    _formatDate: function(date, incomplete) {
        var year = date.getFullYear();
        var month = ("0" + (date.getMonth() + 1)).slice(-2);
        if (!incomplete) {
            var day = ("0" + date.getDate()).slice(-2);
            return [day, month, year].join('/');
        }
        return [month, year].join('/');
    },

    _handleStartPickerChange: function(n, date) {
        console.log('start picker change');
        console.log(date);
        var timestamp = date.getTime(); 
        console.log(timestamp);
        var slider = document.getElementById('date-slider');
        if (this.state.manual)
            slider.noUiSlider.set([timestamp, null]);
    },

    _handleEndPickerChange: function(n, date) {
        console.log(date);
        var timestamp = date.getTime(); 
        console.log(timestamp);
        var slider = document.getElementById('date-slider');
        if (this.state.manual)
            slider.noUiSlider.set([null, timestamp]);
    },

    // TODO: call via ref from app with desired timesteps
    _setFromAnimation: function(startTimestamp, endTimestamp) {
        slider.noUiSlider.set([startTimestamp, endTimestamp]);
    },

    // set minDate and maxDate
    // minDate of startDate needs to be set to rangeSliderStart
    // maxDate of startDate needs to be set one month from currentEndDate
    // maxDate of endDate needs to be set to rangeSliderEnd
    // minDate of endDate needs to be set one month from currentStartDate
    render: function() {
        return (
            <div className="date-overlay">
                <div className="pickers">
                    <DatePicker ref="startPicker" className="date-picker start"
                        formatDate={this._formatDate}
                        style={{ zIndex: 9999 }}
                        hintText="Range Start Date"
                        onChange={this._handleStartPickerChange} />
                    <DatePicker ref="endPicker" className="date-picker end"
                        formatDate={this._formatDate}
                        style={{ zIndex: 9999 }}
                        hintText="Range End Date"
                        onChange={this._handleEndPickerChange} />
                </div>
                <div id="date-slider" />
            </div>
        );       
    }
});