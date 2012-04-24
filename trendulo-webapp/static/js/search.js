var days = 7;
var yAxisPercent = true;
var stateChangeInternal = true;
function performSearch() {

	// grab the search text and tokenize it for the title
	var searchText = $('#searchInputText').val().toLowerCase();
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
	
	// push the query to the browsers history
	stateChangeInternal = true;
	var historyString = "?search=" + searchText + "&days=" + days;
	History.pushState( {}, "", historyString );
	
	// now build the chart with the query
	if ( searchText != undefined && searchText != "" ) {
		buildChart( searchTextEscaped, days, yAxisPercent );
	}
	
	registerDropdowns();
}

function loadTrends() {
	// transform date from mm-dd-yyyy to  yyyymmdd
	var inputDate = $('#trendInputDate').val();
	var dateString = inputDate.substring(6,10) + inputDate.substring(0,2) + inputDate.substring(3,5);
	var url = "trends/DAY/" + dateString;
	$.getJSON(url, function(data){
	  // render HTML with ICanHaz/Mustache Template
      var tbodyHtml = ich.trends( data );
	  $('#trendsDateString').html(data.dateString);
	  $('#trendsTable > tbody').html( tbodyHtml );
	});
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
	$('#60day').click( function() {
		days = 60;
		performSearch();
	});
	$('#90day').click( function() {
		days = 90;
		performSearch();
	});
	$('#180day').click( function() {
		days = 180;
		performSearch();
	});
}

function initializeDatePicker() {
	
	var today = new Date();
	var yesterday = new Date();
	yesterday.setDate( today.getDate() - 1 );
	var month = yesterday.getMonth() + 1
	var day = yesterday.getDate();
	var mm = ( month < 10 ) ? "0" + month : month;
	var dd = ( day < 10 ) ? "0" + day : day;
	var mmddyyyyString = mm + "-" + dd + "-" + yesterday.getFullYear();		
	
	$('#trendsDatePicker').attr('data-date', mmddyyyyString);
	$('#trendInputDate').attr('value', mmddyyyyString);
	$('#trendsDatePicker').datepicker( { format : 'mm-dd-yyyy' } )
		.on('changeDate', function(ev) {
			$('#trendsDatePicker').datepicker('hide');
			loadTrends();
		});
}

// From 999's answer at http://stackoverflow.com/questions/1403888/get-url-parameter-with-jquery
function getURLParameter(name) {
    return decodeURI(
        (RegExp(name + '=' + '(.+?)(&|$)').exec(location.search)||[,""])[1]
    );
}

function initializeSearchFromURL() {
	// If there is a search in the URL it will be in the form trendulo.com/?search=word1[,word2,..]&days=days
	// for example trendulo.com/?search=breakfast&days=7
	var searchParameter=getURLParameter("search");
	var daysParameter=getURLParameter("days");
	if ( (searchParameter !== "") && (daysParameter !== "") ) {
		$('#searchInputText').val( searchParameter );
		
		daysParameterInt = parseInt( daysParameter );
		if ( daysParameterInt == 1 || daysParameterInt == 7 ||
			 daysParameterInt == 30 || daysParameterInt == 60 ||
			 daysParameterInt == 90 || daysParameterInt == 180 ) {
			days = daysParameterInt;	 	
		}
	}
}

function initializeHistory() {
	
	var History = window.History;
	// Bind to StateChange Event
    History.Adapter.bind(window,'statechange',function(){ // Note: We are using statechange instead of popstate
        var State = History.getState(); // Note: We are using History.getState() instead of event.state
        History.log(State.data, State.title, State.url);
        
        // If the statechange event was fired from the browser back/forward, do the search based on the state
        if ( !stateChangeInternal ) {
        	initializeSearchFromURL();
        	performSearch();
        }
        stateChangeInternal = false;
    });
}

 $(document).ready(function(){
 	
	$('#searchForm').submit( function() {
		performSearch();
		return false;
	});

	$('#trendDateForm').submit( function(){
		loadTrends();
		return false;
	});
	
	initializeHistory();
	initializeSearchFromURL();
	initializeDatePicker();		
	performSearch();
	loadTrends();
  });