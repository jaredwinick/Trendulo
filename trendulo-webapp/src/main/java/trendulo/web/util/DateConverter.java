package trendulo.web.util;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DateConverter {

	public static long dateStringToTimestamp( String date ) {
		
		long timestamp = 0;
		DateTimeFormatter formatter = null;
		
		// Month
		if ( date.length() == 6 ) {
			formatter = DateTimeFormat.forPattern("yyyyMM");	
		}
		else if ( date.length() == 8 ) {
			formatter = DateTimeFormat.forPattern("yyyyMMdd");
		}
		else if ( date.length() == 10 ) {
			formatter = DateTimeFormat.forPattern("yyyyMMddHH");
		}
		
		if ( formatter != null ) {
			timestamp = formatter.withZoneUTC().parseMillis( date );
		}
		
		return timestamp;
	}
	
	/**
	 * Get the date string for the start of time period days days ago from timestamp
	 * @param days the number of days in the past from timestamp
	 * @param timestamp the time reference
	 * @return a dateString in the form of yyyyMMdd or yyyyMMddHH
	 */
	public static String getStartDateString( int days, long timestamp ) {
		DateTimeFormatter formatter = getFormatterForDays( days );
		
		// Do the date arithmetic ( timestamp - days )
		DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		DateTime startDateTime = dateTime.minusDays( days ); 
		
		return formatter.withZoneUTC().print( startDateTime );
	}
	
	public static String getEndDateString( int days, long timestamp ) {
		DateTimeFormatter formatter = getFormatterForDays( days );
		DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		return formatter.withZoneUTC().print( dateTime );
	}
	
	private static DateTimeFormatter getFormatterForDays( int days ) {
		DateTimeFormatter formatter = null;
		if ( days > 30 ) { 
			formatter = DateTimeFormat.forPattern("yyyyMMdd");
		}
		else {
			formatter = DateTimeFormat.forPattern("yyyyMMddHH");
		}
		return formatter;
	}
}
