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
	
	// totalRowId is either !NGRAMS or !TWEETS. It is required to be passed in to the ctor
	@Autowired
	public String totalRowId;
	
	private Logger log = Logger.getLogger( CountsController.class );
	
	/*
	@RequestMapping(value = "/counts/{wordsCsvList}/{days}", method = RequestMethod.GET)
	public @ResponseBody List<Series> getCountsSeries( @PathVariable String wordsCsvList, @PathVariable int days ) {
		
		Map<String,SortedMap<String,Long>> wordDateCounters = getWordDateCounters( wordsCsvList, days );;
		
		// Build a List of Series for each word and its date counters
		List<Series> series = new ArrayList<Series>();
		for ( Entry<String, SortedMap<String,Long>> entry : wordDateCounters.entrySet() ) {
			series.add( new Series( entry.getKey(), entry.getValue() ) );
		}
		return series;
	}
	*/
	
	@RequestMapping(value = "/percents/{wordsCsvList}/{days}", method = RequestMethod.GET)
	public @ResponseBody List<Series> getPercentsSeries( @PathVariable String wordsCsvList, @PathVariable int days ) {
		
		// Add the total n-grams key to the list of words
		String wordsCsvListPlusTotal = (wordsCsvList + "," + totalRowId);
		
		Map<String,SortedMap<String,Long>> wordDateCounters = getWordDateCounters( wordsCsvListPlusTotal, days );
		
		// Get the counters for the total n-grams
		SortedMap<String,Long> totalNGramCounters = wordDateCounters.get( totalRowId );
		
		// Build a List of Series for each word and its date counters.
		// Iterate from the original wordsCsvList so the series is returned in the order queried
		List<Series> series = new ArrayList<Series>();
		for ( String word : wordsCsvList.split(",") ) {
			series.add( new Series( word, wordDateCounters.get( word ), totalNGramCounters ) );
		}
		/*
		for ( Entry<String, SortedMap<String,Long>> entry : wordDateCounters.entrySet() ) {
			// only add to the series for real words, not the total n-grams
			if ( !entry.getKey().equals( totalRowId ) ) {
				series.add( new Series( entry.getKey(), entry.getValue(), totalNGramCounters ) );
			}
		}
		*/
		return series;
	}
	
	private Map<String,SortedMap<String,Long>> getWordDateCounters( String wordsCsvList, int days ) {
		log.debug( "Words:" + wordsCsvList );
		log.debug( "Days:" + days );
		
		// words can be a comma seperated list so split
		String [] words = wordsCsvList.split( "," );

		// Get the current time in UTC
		DateTime currentTime = new DateTime( DateTimeZone.UTC );
		String startDateString = DateConverter.getStartDateString( days, currentTime.getMillis() );
		String endDateString = DateConverter.getEndDateString( days, currentTime.getMillis() );
		Map<String,SortedMap<String,Long>> wordDateCounters = queryService.getCounts( words, startDateString, endDateString );
		
		return wordDateCounters;
	}
	
	
}
