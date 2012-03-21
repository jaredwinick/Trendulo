<!DOCTYPE html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width, initial-scale=1.0">
		<title>Trendulo</Title>
		
    	<link href="static/css/bootstrap/bootstrap.css" rel="stylesheet">
    	<style>
     	body {
        	padding-top: 60px; /* 60px to make the container go all the way to the bottom of the topbar */
      	}
    	</style>
    	<link href="static/css/bootstrap/bootstrap-responsive.css" rel="stylesheet">	
		
	</head>
	<body>
	
		<div class="navbar navbar-fixed-top">
      		<div class="navbar-inner">
        		<div class="container">
          			<a class="brand" href="#">Trendulo</a>
          			
			       	<form id="searchForm" class="navbar-search pull-right">
  						<input type="text" id="searchInputText" class="search-query" placeholder="Search" value="breakfast,lunch,dinner">
					</form>
					<ul class="nav pull-right">
  						<li class="dropdown">
   							 <a href="#" class="dropdown-toggle" data-toggle="dropdown">Date Range<b class="caret"></b></a>
    						  <ul class="dropdown-menu">
    							<li><a id="1day" href="#">1 Day</a></li>
    							<li><a id="7day" href="#">7 Days</a></li>
    							<li><a id="30day" href="#">30 Days</a></li>
    							<li><a id="60day" href="#">60 Days</a></li>
    							<li><a id="90day" href="#">90 Days</a></li>
    						</ul>
  						</li>
					</ul>
		  		</div>
			</div>
		</div>

    	<div class="container">

		<div class="row">
			<div class="span7">
				<div class="page-header">
					<h1>Timeline 
						<small id="timelineText">n-gram in last 7 days</small>
					</h1>
				</div>			
			</div>
			<div class="span1">
				<div class="btn-group">
					<a class="btn dropdown-toggle" data-toggle="dropdown" href="#">
						Y-Axis
						<span class="caret"></span>
					</a>
					<ul class="dropdown-menu">
						<li><a id="yAxisPercent" href="#">Percent</a></li>
						<li><a id="yAxisTotalCount" href="#">Total Count</a></li>
					</ul>
				</div>
			</div>
			<div class="span2">
				<div class="page-header">
					<h1>Trends 
						<!--<small id="trendsDateString"></small>-->
					</h1>
				</div>
			</div>
			<div class="span2">
				<form id="trendDateForm" class="well form-search">
  					<input type="text" id="trendInputDate" class="input-mini search-query" placeholder="Date" value="20120319">
  					<!--<button type="submit" class="btn-mini">Search</button>-->
				</form>
			</div>
		</div>
		
		<div class="row">
			<div class="span8">
				<div id="chart"/>
    			</div>
    		</div>
    		<div class="span4">
    			<table id="trendsTable" class="table table-striped table-condensed">
    				<thead>
    					<tr>
    						<th>n-gram</th>
    						<th>score</th>
    					</tr>
    				</thead>
    				<tbody>
    				<!-- Filled in dynamically with template-->
				</tbody>
				</table>
    		</div>
		</div>
	
	
	<script type="text/javascript" src="static/js/jquery-1.7.1/jquery-1.7.1.min.js"></script>
	<script type="text/javascript" src="static/js/bootstrap/bootstrap.min.js"></script>
        <script type="text/javascript" src="static/js/mustache/mustache.js"></script>
	<script type="text/javascript" src="static/js/highstock-1.1.4/js/highstock.src.js"></script>
	<script type="text/javascript" src="static/js/chart.js"></script>
	<script type="text/javascript">
		
		var days = 7;
		var yAxisPercent = true;
		function performSearch() {
			var searchText = escape($('#searchInputText').val().toLowerCase());
			if ( searchText != undefined && searchText != "" ) {
				buildChart( searchText, days, yAxisPercent );
				var yAxisType = "";
				if ( yAxisPercent == true ) {
					yAxisType = "Percent of ";
				} else {
					yAxisType = "Total count of ";
				}
				$('#timelineText').html(yAxisType + "[" + unescape(searchText) + "] in last " + days + " days");
			}
		}
		
		function loadTrends() {
			var url = "/trendulo/trends/DAY/" + $('#trendInputDate').val();
			$.getJSON(url, function(data){
			  //var data = {trends:[{ngram:"test","score":4},{ngram:"cool",score:2}]};
			  var tableTemplate = "{{#trends}}<tr><td>{{ngram}}</td><td>{{score}}</td></tr>{{/trends}}";
                          var tbodyHtml = Mustache.to_html( tableTemplate, data );
			  $('#trendsDateString').html(data.dateString);
			  $('#trendsTable > tbody').html( tbodyHtml );
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
	  		
	  		$('#1day').click( function() {
	  			days = 1;
	  			performSearch();
	  		});
	  		$('#7day').click( function() {
	  			$('#7day').addClass("active");
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
	  		
	  		$('#yAxisPercent').click( function() {
	  			yAxisPercent = true;
	  			performSearch();
	  		});
	  		$('#yAxisTotalCount').click( function() {
	  			yAxisPercent = false;
	  			performSearch();
	  		});
	  		
	  		
	  		performSearch();
			loadTrends();
	 	  });
    </script>
	</body>
</html>
