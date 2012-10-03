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


var days = 7;
var yAxisPercent = true;
var stateChangeInternal = true;
function performSearch() {

	// search text is a hard coded example now
	var searchText = 'breakfast,lunch,dinner'
	var searchTextEscaped = escape(searchText);
	var searchTextTokens = searchText.split(",");
	// build the nGrams array for the template
	var ngrams = [];
	for ( var i = 0; i < searchTextTokens.length; ++i ) {
		var ngramClass = "ngram-" + ( i % 9 );
		var ngram = { "ngram" : searchTextTokens[i], "ngramClass" : ngramClass };
		ngrams.push( ngram );
	}
	// now build the data object for the template
	var data = { "ngrams" : ngrams, "days" : days };
	
	// set the text of the Timeline title
	var tbodyHtml = ich.timelineTitle( data );
	 $('#timelineTitleTable > tbody').html( tbodyHtml );
	
	// now build the chart with the query
	if ( searchText != undefined && searchText != "" ) {
		buildChart( days );
	}
	
	registerDropdowns();
}


function registerDropdowns() {
	$('#1day').click( function() {
		days = 1;
		performSearch();
	});
	$('#7day').click( function() {
		days = 7;
		performSearch();
	});
	$('#30day').click( function() {
		days = 30;
		performSearch();
	});
}

$(document).ready(function(){
	performSearch();
});
