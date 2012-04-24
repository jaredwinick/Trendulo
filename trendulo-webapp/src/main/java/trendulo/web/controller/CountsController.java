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

import trendulo.web.date.DateConverter;
import trendulo.web.model.Series;
import trendulo.web.query.QueryService;

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
	
	@RequestMapping(value = "/timeline/{wordsCsvList}/{days}", method = RequestMethod.GET)
	public @ResponseBody List<Series> getPercentsSeries( @PathVariable String wordsCsvList, @PathVariable int days ) {
		
		// Add the total n-grams key to the list of words
		String wordsCsvListPlusTotal = (wordsCsvList + "," + totalRowId);
		
		// Get the current time in UTC and get the start and stop date strings
		DateTime currentTime = new DateTime( DateTimeZone.UTC );
		String startDateString = DateConverter.getStartDateString( days, currentTime.getMillis() );
		String endDateString = DateConverter.getEndDateString( days, currentTime.getMillis() );
		
		Map<String,SortedMap<String,Long>> wordDateCounters = getWordDateCounters( wordsCsvListPlusTotal, startDateString, endDateString, days );
		
		// fill in any empty dateCounters with 0 for each word
		List<String> dateStrings = DateConverter.getDateStringsForRange(startDateString, endDateString, days);
		for ( Entry<String,SortedMap<String,Long>> entry : wordDateCounters.entrySet() ) {
			for ( String dateString : dateStrings ) {
				if ( !entry.getValue().containsKey(dateString) ) {
					entry.getValue().put( dateString, new Long(0) );
				}
			}
		}
		
		// Get the counters for the total n-grams
		SortedMap<String,Long> totalNGramCounters = wordDateCounters.get( totalRowId );
		
		// Build a List of Series for each word and its date counters.
		// Iterate from the original wordsCsvList so the series is returned in the order queried
		List<Series> series = new ArrayList<Series>();
		for ( String word : wordsCsvList.split(",") ) {
			String trimmedWord = word.trim();
			series.add( new Series( trimmedWord, wordDateCounters.get( trimmedWord ), totalNGramCounters ) );
		}

		return series;
	}
	
	private Map<String,SortedMap<String,Long>> getWordDateCounters( String wordsCsvList, String startDateString, String endDateString, int days ) {
		log.debug( "Words:" + wordsCsvList );
		log.debug( "Days:" + days );
		
		// words can be a comma seperated list so split
		String [] words = wordsCsvList.split( "," );
		// trim leading and trailing white space from each word
		for ( int i = 0 ; i < words.length ; ++i ) {
			words[i] = words[i].trim();
		}
		Map<String,SortedMap<String,Long>> wordDateCounters = queryService.getCounts( words, startDateString, endDateString );
		
		return wordDateCounters;
	}
	
	
}
