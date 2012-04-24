package trendulo.web.date;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class DateConverter {

	// i don't like this. we really need to get everything back to UTC
	private static DateTimeZone mdtDateTimeZone = DateTimeZone.forOffsetHours(-6);
	
	public static long dateStringToTimestamp( String date ) {
		
		return dateStringToTimestamp( date, DateTimeZone.UTC);
	}
	
	public static long dateStringToTimestamp( String date, DateTimeZone dateTimeZone ) {
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
			timestamp = formatter.withZone( dateTimeZone ).parseMillis( date );
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
		//DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		DateTime dateTime = new DateTime( timestamp, mdtDateTimeZone );
		DateTime startDateTime = dateTime.minusDays( days ); 
		
		//return formatter.withZoneUTC().print( startDateTime );
		return formatter.withZone( mdtDateTimeZone ).print( startDateTime );
	}
	
	public static String getEndDateString( int days, long timestamp ) {
		DateTimeFormatter formatter = getFormatterForDays( days );
		//DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		DateTime dateTime = new DateTime( timestamp, mdtDateTimeZone );
		//return formatter.withZoneUTC().print( dateTime );
		return formatter.withZone( mdtDateTimeZone ).print( dateTime );
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
	
	public static List<String> getDateStringsForRange( String startDateString, String endDateString, int days ) {
		
		List<String> dateStrings = new ArrayList<String>();
		
		long startDate = dateStringToTimestamp( startDateString, mdtDateTimeZone );
		long endDate = dateStringToTimestamp( endDateString, mdtDateTimeZone );
		
		// determine the dateIncrement. It is either 1 hour or 1 day
		long dateIncrement = 0;
		if ( startDateString.length() == 8 ) {
			dateIncrement = 1 * 24 * 60 * 60 * 1000;  // 1 day in milliseconds
		}
		else if ( startDateString.length() == 10 ) {
			dateIncrement = 1 * 60 * 60 * 1000;  // 1 hour in milliseconds
		}
		
		// loop through all the dates and build the list of date strings
		for ( long currentDate = startDate; currentDate <= endDate; currentDate += dateIncrement ) {
			dateStrings.add( getEndDateString( days, currentDate ) );
		}
		
		return dateStrings;		
	}
}
