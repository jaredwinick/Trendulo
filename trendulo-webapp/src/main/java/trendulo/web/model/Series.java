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


package trendulo.web.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import trendulo.web.date.DateConverter;

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
			// Avoid a divide by 0 if we simply don't have data for this time
			if ( total != 0 ) {
				data.add( new Double [] { new Double(timestamp), new Double((double)dateCounter.getValue() / (double)total) * 100d } );
			}
			else {
				data.add( new Double [] { new Double(timestamp), 0d } );
			}
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
