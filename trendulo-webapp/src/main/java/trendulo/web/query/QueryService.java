package trendulo.web.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

public class QueryService {

	@Autowired
	private Instance instance;
	@Autowired
	private Connector connector;
	@Autowired
	private BatchScanner tweetsBatchScanner;
	@Autowired
	private Scanner trendsScanner;
	
	private Logger log = Logger.getLogger( QueryService.class );
	
	public QueryService(  ) {

	}

	public SortedMap<Integer, String> getTrends( String dateString, int limit ) {
		SortedMap<Integer, String> trends = new TreeMap<Integer, String>();
		
		String dateGranularity = (dateString.length() == 8 ) ? "DAY" : "HOUR";
		trendsScanner.setRange( new Range(dateGranularity + ":" + dateString) );
		int count = 0;
		for ( Entry<Key,Value> entry : trendsScanner ) {
			// the score is the column family, the ngram is the column qual
			log.debug(entry.getKey().toStringNoTime());
	
			log.debug("score in db:" + Integer.parseInt( entry.getKey().getColumnFamily().toString() ));
			Integer score = Integer.MAX_VALUE - Integer.parseInt( entry.getKey().getColumnFamily().toString() );
			String ngram = entry.getKey().getColumnQualifier().toString();
			trends.put(score, ngram);
			// Pull off the top limit scores - we are reading them in sorted order
			log.debug("Count:" + count + " limit:" + limit);
			if ( count++ >= limit ) {
				break;
			}		
		}
		return trends;
	}
	
	public Map<String, SortedMap<String,Long>> getCounts( String [] words, String startDateString, String endDateString ) {
		
		Map<String,SortedMap<String,Long>> wordDateCounters = new HashMap<String, SortedMap<String,Long>>();
		// set the granularity based on the length of the string passed in
		String dateGranularity = ( startDateString.length() == 8 ) ? "DAY" : "HOUR";
		
		tweetsBatchScanner.clearColumns();
		List<Range> ranges = new ArrayList<Range>();
		for ( String word : words ) {
			Key startKey = new Key( new Text( word ), new Text( dateGranularity ), new Text( startDateString ) );
			// We want the endKey to include the endDateString, so we will build the Key just following it but then
			// set the Range to be exclusive. Simply using the endDateString and making the Range inclusive doesn't
			// work because of the default Value of the Key's timestamp (Long.MAX_VALUE)
			// See "Bug in Range behavior?" message on Accumulo user mailing list on Mar 22, 2012
			Key endKey = new Key( new Text( word ), new Text( dateGranularity ), new Text( endDateString ) ).followingKey(PartialKey.ROW_COLFAM_COLQUAL);
			log.debug( "Start Key: " + startKey.toStringNoTime() );
			log.debug( "End Key: " + endKey.toStringNoTime() );
			Range range = new Range( startKey, true, endKey, false );
			ranges.add( range );
		}
		
		tweetsBatchScanner.setRanges( ranges );
		
		// Get the results. As we are using the BatchScanner, these aren't in order
		for ( Entry<Key,Value> entry : tweetsBatchScanner ) {
			
			// Get the word as that will be the key to the Map
			String word = entry.getKey().getRow().toString();
			Long count = 0l;
			
			// Check is we have a SortedMap for the word yet. The sorted map is a list of date->count
			SortedMap<String,Long> dateCounters = wordDateCounters.get( word );
			if ( dateCounters == null ) {
				dateCounters = new TreeMap<String,Long>();
				wordDateCounters.put( word, dateCounters ); 
			}
			// Get the date value which is the CQ
			String dateValue = entry.getKey().getColumnQualifier().toString();
			
			// Now get the counter value
			try {
				count = Value.bytesToLong( entry.getValue().get() );
			} catch (IOException e) {
				log.error( "Can't parse Value to long", e );
			}
			
			// add the date and count to the dateCounters SortedMap
			dateCounters.put( dateValue, count );
		}
		
		return wordDateCounters;
	}
}
