const { LeftNav, FlatButton } = mui;
const { Menu, MenuItem, MenuDivider, ArrowDropRight } = mui.Menus;

// this: https://github.com/callemall/material-ui/issues/744
CommodityControls = React.createClass({

    componentDidMount: function() {
        console.log('mounted');
        //this.refs.leftNav.toggle();
    },

    shouldComponentUpdate: function(nextProps, nextState) {
        if (this.props.open !== nextProps.open) {
            this._toggleNav();
            return false;
        }
        return true;
    },

    _onSelect: function(e, selectedIndex, menuItem) {
        console.log(e);
        console.log(selectedIndex);
        console.log(menuItem);
        this.props.handleSelection(menuItem.text);
    },

    _toggleNav: function() {
        if (this.refs) {
            this.refs.leftNav.toggle();
        }
    },

    _onClose: function() {
        console.log('Fire!');
        this.props.handleHide();
    },

    _hideClick: function() {
        this.props.handleHide();
    },

    _createMenu: function() {
        // list generated from category-commodity object from the India document in States containing all commodities pre-selected for the visualization
        // TODO: if visType production => mark commodities red for which no production data exists (add a boolean field to every commodity to indicate whether it is recorded or make an additional query to production ?)
        // if this.props.visType === 'production'
        var menuItems = [
            { text: 'All Commodities' }
        ];
        if (this.props.loading)
            return menuItems;

        var subheaderType = mui.MenuItem.Types.SUBHEADER; 
        catCommObj = this.props.commodities[0].commodities;
        for (var cat in catCommObj) {
            var comms = catCommObj[cat]; 
            menuItems.push({ type: subheaderType, text: cat});
            for (var i in comms) {
                menuItems.push({ text: comms[i] });
            }
        }
        return menuItems;
    },

    render: function() {
        menuItems = this._createMenu();
        // on touchtap -> close navbar
        let hideButton = <FlatButton key={1} label="Hide" primary={true} onTouchTap={this._hideClick} />;
        return (
            <LeftNav ref="leftNav" header={hideButton} docked={false} menuItems={menuItems} style={{zIndex: 9999 }} onChange={this._onSelect} onNavClose={this._onClose} />
        );    
    }
});