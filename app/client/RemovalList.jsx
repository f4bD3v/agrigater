//http://stackoverflow.com/questions/27817241/how-to-remove-an-item-from-a-list-with-a-click-event-in-reactjs
// selectable list: http://www.material-ui.com/#/components/lists
const { List, ListItem, ListDivider } = mui
const { ActionDelete } = mui.SvgIcons;

RemovalList = React.createClass({
    _compare: function(a, b, field) {
        if (a[field] < b[field])
            return -1;
        if (a[field] > b[field])
            return 1;
        return 0;
    },

    _sortData: function () {
        var data = this.props.seriesDescriptions.slice(0);
        // http://ryanmorr.com/true-hash-maps-in-javascript/ 
        
        data.sort(function(a, b) { 
            this._compare(a, b, "adminUnit"); 
        });
    },

    // somehow get the clear icon: https://www.google.com/design/icons/
    _createItem: function(item) {
        //leftIcon={<Delete />} 
        var text = [item.name, item.commodity].join(': ')
        var classN = [item.adminUnit, 'item'].join('-')
        var deleteIcon = <CustomActionDelete color={item.color} />
        // on click icon do something, or on click list?
        return (
            <ListItem leftIcon={deleteIcon} primaryText={text} className={classN} key={item.id} style={{ marginBottom: 5 }} onClick={this.props.onDelete.bind(null, item)} />
        );
    },

    _getSectionItems: function(adminUnit) {
        var listSection = [];
        console.log(adminUnit);
        console.log(this.props.selected);
        console.log(this.props.selected[0]);
        var selectedItems = this.props.selected.filter(function(obj) { return obj.adminUnit === adminUnit; })[0].selected;
        console.log(selectedItems);
        // NOTE: [] selectedItems should run through!
        return selectedItems;
        /*
        console.log(selectedItems);
        //var divider = firstSection? '' : <ListDivider />;
        self = this;
        //var count = -1;
        for (i in selectedItems) {
            listSection.push(self._createItem(selectedItems[i], adminUnit, i))
        }
        /*var listSection = selectedItems.map(function(item) { 
            count+=1;
            return self._createItem(item, adminUnit, count);
        })
        console.log(listSection);
        return listSection;
        */
    },

    render: function() {
        // selectedSeries is never empty
        // <div />, <div></div> is equivalent
        var ifdist = this.props.selected.length > 1;
        var ifmarket = this.props.selected.length > 2;
        //{ ifdist ? this._createRemovalList('district') : '' }
        //{ ifmarket ? <ListDivider/> : '' }
        //{ ifmarket ? this._createRemovalList('market') : '' }
        var self = this;
        return (
            <div className="removalContainer">
                <h4>Remove Series</h4>
                <List>
                { this._getSectionItems('state').map(function(item) { return self._createItem(item) }) }
                { ifdist ? <ListDivider/> : '' }
                { ifdist ? this._getSectionItems('district').map(function(item) { return self._createItem(item) }) : '' }
                { ifmarket ? <ListDivider/> : '' }
                { ifmarket ? this._getSectionItems('market').map(function(item) { return self._createItem(item) }) : '' }
                </List>
            </div>
        );
    }
});