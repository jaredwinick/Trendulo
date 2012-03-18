package trendulo.ingest.twitter;

import twitter4j.Status;

public class USCountryCodeStatusFilter implements StatusFilter {

	@Override
	public boolean accept(Status status) {

		if ( status.getPlace() != null &&  status.getPlace().getCountryCode() != null && status.getPlace().getCountryCode().equals("US") ) {
			return true;
		}
		return false;
		
	}

}
