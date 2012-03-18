function buildChart( searchText, days ) {
	$.getJSON('/trendulo/counts/' + searchText + '/' + days, function(data) {
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
			}
		});
	});
}