<script src="https://code.highcharts.com/stock/highstock.js"></script>
<script src="https://code.highcharts.com/stock/modules/exporting.js"></script>
<div id="container" style="height: 400px; min-width: 310px"></div>

$(function () {
    var series1=[5,6,7,4,3,2,1,0,5,5.5,4,3.5,4,5.5,6];
    var series2=[5.5,5,4.5,4,4.5,5,6,6.5,7,10,10.5,9,8,7.5,7.5];
    var column1 = [30,40,50,55,45,60,70,80,100,60,40,30,40,50,15];
    var column2 = column1.slice(0).reverse();
    $('#container').highcharts('StockChart', {
        rangeSelector: {
            selected: 1
        },
        title: {
            text: 'My badass range chart'
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
            }
        },
        series: [{
            type: 'line',
            name: 'Series1',
            data: series1,
            yAxis: 0
        }, {
            type: 'line',
            name: 'Series2',
            data: series2,
            yAxis: 0
        }, {
            type: 'column',
            name: 'Tonnes1',
            data: column1,
            yAxis: 1
        }, {
            type: 'column',
            name: 'Tonnes2',
            data: column2,
            yAxis: 1
        }]
    });
});