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
import org.apache.accumulo.core.data.Key;
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
	private BatchScanner batchScanner;
	
	private Logger log = Logger.getLogger( QueryService.class );
	
	public QueryService(  ) {

	}

	public Map<String, SortedMap<String,Long>> getCounts( String [] words, String startDateString, String endDateString ) {
		
		Map<String,SortedMap<String,Long>> wordDateCounters = new HashMap<String, SortedMap<String,Long>>();
		// set the granularity based on the length of the string passed in
		String dateGranularity = ( startDateString.length() == 8 ) ? "DAY" : "HOUR";
		
		batchScanner.clearColumns();
		List<Range> ranges = new ArrayList<Range>();
		for ( String word : words ) {
			Key startKey = new Key( new Text( word ), new Text( dateGranularity ), new Text( startDateString ) );
			Key endKey = new Key( new Text( word ), new Text( dateGranularity ), new Text( endDateString ) );
			Range range = new Range( startKey, endKey );
			ranges.add( range );
		}
		
		batchScanner.setRanges( ranges );
		
		// Get the results. As we are using the BatchScanner, these aren't in order
		for ( Entry<Key,Value> entry : batchScanner ) {
			
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
