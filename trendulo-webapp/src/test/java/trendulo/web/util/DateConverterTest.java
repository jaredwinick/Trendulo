package trendulo.web.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class DateConverterTest {

	@Test
	public void dateStringToTimestampMonth() {
		long timestamp = DateConverter.dateStringToTimestamp("201203");
		assertEquals( timestamp, 1330560000000l );
	}
	
	@Test
	public void dateStringToTimestampDay() {
		long timestamp = DateConverter.dateStringToTimestamp("20120302");
		assertEquals( timestamp, 1330646400000l );
	}
	
	@Test
	public void dateStringToTimestampHour() {
		long timestamp = DateConverter.dateStringToTimestamp("2012030201");
		assertEquals( timestamp, 1330650000000l );
	}
	
	@Test 
	public void getStartDateString1Days() {
		// timestamp is Human time (GMT): Fri, 02 Mar 2012 01:00:00 GMT
		String startDate = DateConverter.getStartDateString( 1, 1330650000000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString7Days() {
		// timestamp is Human time (GMT): Thu, 08 Mar 2012 01:00:00 GMT
		String startDate = DateConverter.getStartDateString( 7, 1331168400000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString30Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String startDate = DateConverter.getStartDateString( 30, 1333155600000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString60Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String startDate = DateConverter.getStartDateString( 60, 1333155600000l );
		assertEquals( startDate, "20120131" );
	}
	@Test 
	public void getStartDateString90Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String startDate = DateConverter.getStartDateString( 90, 1333155600000l );
		assertEquals( startDate, "20120101" );
	}
	
	@Test 
	public void getEndDateString1Days() {
		// timestamp is Human time (GMT): Fri, 02 Mar 2012 01:00:00 GMT
		String endDate = DateConverter.getEndDateString( 1, 1330650000000l );
		assertEquals( endDate, "2012030201" );
	}
	@Test 
	public void getEndDateString7Days() {
		// timestamp is Human time (GMT): Thu, 08 Mar 2012 01:00:00 GMT
		String endDate = DateConverter.getEndDateString( 7, 1331168400000l );
		assertEquals( endDate, "2012030801" );
	}
	@Test 
	public void getEndDateString30Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String endDate = DateConverter.getEndDateString( 30, 1333155600000l );
		assertEquals( endDate, "2012033101" );
	}
	@Test 
	public void getEndDateString60Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String endDate = DateConverter.getEndDateString( 60, 1333155600000l );
		assertEquals( endDate, "20120331" );
	}
	@Test 
	public void getEndDateString90Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		String endDate = DateConverter.getEndDateString( 90, 1333155600000l );
		assertEquals( endDate, "20120331" );
	}
}
