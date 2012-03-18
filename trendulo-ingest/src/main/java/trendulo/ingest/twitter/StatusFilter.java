package trendulo.ingest.twitter;

import twitter4j.Status;

public interface StatusFilter {
	
	/**
	 * Accept is used to filter Status objects. 
	 * @param status Twitter4J Status object
	 * @return true if the Status is accepted, false if it is rejected and should be filtered out
	 */
	public boolean accept( Status status );
}
