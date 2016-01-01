const { Toolbar, ToolbarGroup, ToolbarTitle, ToolbarSeparator, FlatButton, Checkbox} = mui;
const { Menu, MenuItem, MenuDivider, ArrowDropRight } = mui.Menus;
const { NavigationArrowDropRight, ActionDelete } = mui.SvgIcons;

VisModeControls = React.createClass({
    getInitialProps: function() {
      return {
        checkbox: true,
      } 
    },
      /*
        <Menu desktop={true} width={320}>
        <MenuItem primaryText="Bold" secondaryText="&#8984;B" rightIcon={<ActionDelete fill={"#ff6600"} />} />
        <MenuDivider />
        <MenuItem primaryText="Paragraph styles" rightIcon={<NavigationArrowDropRight />} />
      </Menu>
      */
    _onClick: function(visType) {
      console.log(visType);
      this.props.setVisType(visType);
    },

    _onCheck: function(event, checked) {
      // NOTE: checked in [true, false]
      this.props.onCheck(checked);
    },

    render: function() {
      var left = $(document).width() * .5 - 400;
      // checkbox disabled depends on visType prop passed from app
      return (
        <Toolbar style={{ position: 'absolute', zIndex: 1000, backgroundColor: "#fff" }} top={0} left={0}>
        <ToolbarGroup key={0} className="mode-menu">
          <ToolbarTitle text="Modes:" />
          <FlatButton label="Trade" primary={true} onTouchTap={this._onClick.bind(null, 'trade')} />
          <ToolbarSeparator/>
          <FlatButton label="Production" primary={true} onTouchTap={this._onClick.bind(null, 'production')} />
          <ToolbarSeparator/>
          <FlatButton label="Time Series" primary={true} onTouchTap={this._onClick.bind(null, 'timeSeries')} />
          <Checkbox className="compare" name="compare" value="compare" label="compare" disabled={!this.props.checkbox} onCheck={this._onCheck} /> 
        </ToolbarGroup>
        </Toolbar> 
      );
    }
});