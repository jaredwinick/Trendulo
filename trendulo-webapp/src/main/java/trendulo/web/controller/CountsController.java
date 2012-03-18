package trendulo.web.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import trendulo.web.model.Series;
import trendulo.web.query.QueryService;
import trendulo.web.util.DateConverter;

@Controller
public class CountsController {
	
	@Autowired
	private QueryService queryService;
	
	private Logger log = Logger.getLogger( CountsController.class );
	
	@RequestMapping(value = "/counts/{wordsCsvList}/{days}", method = RequestMethod.GET)
	public @ResponseBody List<Series> getSeries( @PathVariable String wordsCsvList, @PathVariable int days ) {
		
		log.debug( "Words:" + wordsCsvList );
		log.debug( "Days:" + days );
		
		// words can be a comma seperated list so split
		String [] words = wordsCsvList.split( "," );

		// Get the current time in UTC
		DateTime currentTime = new DateTime( DateTimeZone.UTC );
		String startDateString = DateConverter.getStartDateString( days, currentTime.getMillis() );
		String endDateString = DateConverter.getEndDateString( days, currentTime.getMillis() );
		Map<String,SortedMap<String,Long>> wordDateCounters = queryService.getCounts( words, startDateString, endDateString );
		
		// Build a List of Series for each word and its date counters
		List<Series> series = new ArrayList<Series>();
		for ( Entry<String, SortedMap<String,Long>> entry : wordDateCounters.entrySet() ) {
			series.add( new Series( entry.getKey(), entry.getValue() ) );
		}
		return series;
	}
}
