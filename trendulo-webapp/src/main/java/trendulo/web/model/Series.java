package trendulo.web.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import trendulo.web.util.DateConverter;

public class Series {
	
	private final String DEFAULT_TYPE = "line";
	
	private String name;
	private List<Double[]> data;
	private String type;
	
	public Series() {}
	
	/**
	 * Build a Series for a given name and list of date,counter pairs
	 * @param name The name associated with the dateCounters
	 * @param dateCounters a sorted list of date,counter pairs
	 */
	public Series( String name, SortedMap<String, Long> dateCounters ) {
		this.name = name;
		this.type = DEFAULT_TYPE;
		data = new ArrayList<Double[]>();
		for ( Entry<String, Long> dateCounter : dateCounters.entrySet() ) {
			long timestamp = DateConverter.dateStringToTimestamp( dateCounter.getKey() );
			data.add( new Double [] { new Double(timestamp), new Double(dateCounter.getValue()) } );
		}
	}
	
	/**
	 * Build a Series for a given name and list of date,counter pairs. The value is a percent of the total count 
	 * @param name The name associated with the dateCounters
	 * @param dateCounters a sorted list of date,counter pairs
	 */
	public Series( String name, SortedMap<String, Long> dateCounters, SortedMap<String, Long> totalCounters ) {
		this.name = name;
		this.type = DEFAULT_TYPE;
		data = new ArrayList<Double[]>();
		for ( Entry<String, Long> dateCounter : dateCounters.entrySet() ) {
			long timestamp = DateConverter.dateStringToTimestamp( dateCounter.getKey() );
			// get the total count for this date
			long total = totalCounters.get( dateCounter.getKey() );
			data.add( new Double [] { new Double(timestamp), new Double((double)dateCounter.getValue() / (double)total) * 100d } );
		}
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Double[]> getData() {
		return data;
	}

	public void setData(List<Double[]> data) {
		this.data = data;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
