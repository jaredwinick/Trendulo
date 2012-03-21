function buildChart( searchText, days, yAxisPercent ) {
	var url;
	if ( yAxisPercent == true ) {
		url = '/trendulo/percents/' + searchText + '/' + days;
	}
	else {
		url = '/trendulo/counts/' + searchText + '/' + days;
	}
	$.getJSON( url, function(data) {
		var chart = new Highcharts.StockChart({
			chart : {
				renderTo: "chart"
			},
			title: {
				text: ''
			},
			series: data,
			credits : {
				enabled : false
			},
			rangeSelector : {
				enabled : false
			},
			tooltip : {
				yDecimals : 4
			}
		});
	});
}