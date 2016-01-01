const { Toolbar, ToolbarGroup, ToolbarTitle, ToolbarSeparator, RaisedButton } = mui;
const { Menu, MenuItem, MenuDivider, ArrowDropRight } = mui.Menus;
const { NavigationArrowDropRight, ActionDelete } = mui.SvgIcons;

VisControls = React.createClass({
      /*
      <Menu desktop={true} width={320}>
        <MenuItem primaryText="Bold" secondaryText="&#8984;B" rightIcon={<ActionDelete fill={"#ff6600"} />} />
        <MenuDivider />
        <MenuItem primaryText="Paragraph styles" rightIcon={<NavigationArrowDropRight />} />
      </Menu>
      */
    render: function() {
        return (
      <Toolbar style={{ position: 'absolute', zIndex: 1000, backgroundColor: "#fff" }} top={0} left={0}>
      <ToolbarGroup key={0} float="left">
        <ToolbarTitle text="Visualization Modes:" />
        <ToolbarSeparator/>

        <RaisedButton label="Create Broadcast" primary={true} />
      </ToolbarGroup>

      </Toolbar> 
      );
    }
});