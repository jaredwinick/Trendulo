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
  						<input type="text" id="searchInputText" class="search-query" placeholder="Search">
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
			<div class="span8">
				<div class="page-header">
					<h1>Timeline 
						<small id="timelineText">n-gram in last 7 days</small>
					</h1>
				</div>			
			</div>
			<div class="span4">
				<div class="page-header">
					<h1>Trends 
						<small>March 03, 2012</small>
					</h1>
				</div>
			</div>
		</div>
		
		<div class="row">
			<div class="span8">
				<div id="chart"/>
    			</div>
    		</div>
    		<div class="span4">
    			<table class="table table-striped">
    				<thead>
    					<tr>
    						<th>n-gram</th>
    						<th>score</th>
    					</tr>
    				</thead>
    				<tbody>
    					<tr>
    						<td>jared</td>
    						<td>1923</td>
    					</tr>
    					<tr>
    						<td>stacy</td>
    						<td>323</td>
    					</tr>
    					<tr>
    						<td>pooch</td>
    						<td>19</td>
    					</tr>
    				</tbody>
				</table>
    		</div>
		</div>
	
	
	<script type="text/javascript" src="static/js/jquery-1.7.1/jquery-1.7.1.min.js"></script>
	<script type="text/javascript" src="static/js/bootstrap/bootstrap.min.js"></script>
	<script type="text/javascript" src="static/js/highstock-1.1.4/js/highstock.src.js"></script>
	<script type="text/javascript" src="static/js/chart.js"></script>
	<script type="text/javascript">
		
		var days = 1;
		function performSearch() {
			var searchText = $('#searchInputText').val();
			if ( searchText != undefined && searchText != "" ) {
				buildChart( searchText, days );
				
				$('#timelineText').html("[" + searchText + "] in last " + days + " days");
			}
		}
		
    	 $(document).ready(function(){
	  		$('#searchForm').submit( function() {
	  			performSearch();
	  			return false;
	  		});
	  		
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
	  		
	  		performSearch();
	 	  });
    </script>
	</body>
</html>