$(document).ready(function(){
	
	$('#searchForm').submit( function() {
		// build the url that we will redirect to. default to 7 day search
		var searchText = $('#searchInputText').val().toLowerCase();
		var searchTextEscaped = escape(searchText);
		var url = "/?search=" + searchTextEscaped + "&days=7";
		window.location = url;
		return false;
	});
	
});