// =================================================================================================
// Copyright 2012 Jared Winick
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================

function buildChart( days ) {
	var url = 'data/bld_' + days + '.json';

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
