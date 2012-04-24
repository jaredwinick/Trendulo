function buildChart( searchText, days ) {
	var url = 'timeline/' + searchText + '/' + days;

	$.getJSON( url, function(data) {
		var chart = new Highcharts.StockChart({
			chart : {
				renderTo: "chart"
			},
			title: {
				text: ''
			},
			yAxis : {
				min: 0
			},
			series: data,
			credits : {
				enabled : false
			},
			navigator : {
				enabled : true,
				baseSeries: 0
			},
			rangeSelector : {
				enabled : false
			},
			tooltip : {
				yDecimals : 3
			}
		});
	});
}