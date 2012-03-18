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
	private List<Long[]> data;
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
		data = new ArrayList<Long[]>();
		for ( Entry<String, Long> dateCounter : dateCounters.entrySet() ) {
			long timestamp = DateConverter.dateStringToTimestamp( dateCounter.getKey() );
			data.add( new Long [] { timestamp, dateCounter.getValue() } );
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Long[]> getData() {
		return data;
	}

	public void setData(List<Long[]> data) {
		this.data = data;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
