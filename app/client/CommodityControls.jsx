const { LeftNav, FlatButton, List, ListItem } = mui;
//const { Menu, MenuItem, MenuDivider} = mui.Menus;

// this: https://github.com/callemall/material-ui/issues/744
CommodityControls = React.createClass({

    getInitialState: function() {
        return {
            open: false
        };
    },

    componentDidMount: function() {
        console.log('mounted');
        //this.refs.leftNav.toggle();
    },

    shouldComponentUpdate: function(nextProps, nextState) {
        if (this.props.open !== nextProps.open) {
            //this._toggleNav();
            // nextProps.open => open
            this.setState({ open: !this.state.open });
            //this._toggleNav(nextProps.open, 'update');
            return false;
        }
        return true;
    },

    // TODO: call with menuitem on touchtap
    _onClick: function(event) {
        console.log(event.target.textContent);
        this.props.handleSelection(event.target.textContent);
    },

    /* deprecated
    _onSelect: function(e, selectedIndex, menuItem) {
        console.log(e);
        console.log(selectedIndex);
        console.log(menuItem);
        this.props.handleSelection(menuItem.text);
    },*/

    _toggle: function(open, reason) {
        // toggle with open: !this.state.open
        console.log(open);
        console.log(reason);
        if (!open)
            this.props.handleHide();
        this.setState({ open: open });
        /*
        if (open) {
            this.setState({open: false})
            this.props.handleHide();
        } else {
            this.setState({open: true})
        }*/
    },

    /* deprecated with new version of leftNav */
    /*
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
    },*/

    _hideClick: function() {
        this.props.handleHide();
    },

    /*
    _createMenu: function() {
        // list generated from category-commodity object from the India document in States containing all commodities pre-selected for the visualization
        // TODO: if visType production => mark commodities red for which no production data exists (add a boolean field to every commodity to indicate whether it is recorded or make an additional query to production ?)
        // if this.props.visType === 'production'
        /* DEPRECATED
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
    */

    _createMenu: function() {
        lists = [];
        return lists;
    },

    render: function() {
        // menuItems = this._createMenu();
        // on touchtap -> close navbar
        //<LeftNav ref="leftNav" header={hideButton} docked={false} menuItems={menuItems} style={{zIndex: 9999 }} onChange={this._onSelect} onRequestChange={this._onClose} />
        let hideButton = <FlatButton key={1} label="Hide" primary={true} onTouchTap={this._hideClick} />;
        // put picture of commodity as left icon leftIcon={<ContentDrafts />}
        let catCommObj = {};
        console.log(this.props.commodities);
        if (this.props.commodities.length > 0) {
            catCommObj = this.props.commodities[0].commodities;
            const ordered = {};
            Object.keys(catCommObj).sort().forEach(function(key) {
                ordered[key] = catCommObj[key];
            });
            catCommObj = ordered;
        }
        // TODO:  SelectableContainerEnhance
        // http://www.material-ui.com/#/components/lists
        var self = this;
        return (
            <LeftNav ref="leftNav" open={this.state.open} docked={false} style={{zIndex: 9999, overflow: "scroll" }} onRequestChange={this._toggle}>
                {hideButton}
                <List>
                    <ListItem primaryText="All Commodities" onTouchTap={self._onClick} />
                </List>
                {_.map(catCommObj, function(comms, cat) { return <List subheader={cat}>{_.map(comms, function(comm) { return <ListItem key={comm} primaryText={comm} onTouchTap={self._onClick} /> })}</List> })}
            </LeftNav>
        );    
    }
});