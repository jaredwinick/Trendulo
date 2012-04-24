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


package trendulo.web.util;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import trendulo.web.date.DateConverter;

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
		//String startDate = DateConverter.getStartDateString( 1, 1330650000000l );
		String startDate = DateConverter.getStartDateString( 1, 1330650000000l + 21600000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString7Days() {
		// timestamp is Human time (GMT): Thu, 08 Mar 2012 01:00:00 GMT
		//String startDate = DateConverter.getStartDateString( 7, 1331168400000l );
		String startDate = DateConverter.getStartDateString( 7, 1331168400000l + 21600000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString30Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String startDate = DateConverter.getStartDateString( 30, 1333155600000l );
		String startDate = DateConverter.getStartDateString( 30, 1333155600000l + 21600000l );
		assertEquals( startDate, "2012030101" );
	}
	@Test 
	public void getStartDateString60Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String startDate = DateConverter.getStartDateString( 60, 1333155600000l );
		String startDate = DateConverter.getStartDateString( 60, 1333155600000l + 21600000l );
		assertEquals( startDate, "20120131" );
	}
	@Test 
	public void getStartDateString90Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String startDate = DateConverter.getStartDateString( 90, 1333155600000l );
		String startDate = DateConverter.getStartDateString( 90, 1333155600000l + 21600000l );
		assertEquals( startDate, "20120101" );
	}
	
	@Test 
	public void getEndDateString1Days() {
		// timestamp is Human time (GMT): Fri, 02 Mar 2012 01:00:00 GMT
		//String endDate = DateConverter.getEndDateString( 1, 1330650000000l );
		String endDate = DateConverter.getEndDateString( 1, 1330650000000l + 21600000l );
		assertEquals( endDate, "2012030201" );
	}
	@Test 
	public void getEndDateString7Days() {
		// timestamp is Human time (GMT): Thu, 08 Mar 2012 01:00:00 GMT
		//String endDate = DateConverter.getEndDateString( 7, 1331168400000l );
		String endDate = DateConverter.getEndDateString( 7, 1331168400000l + 21600000l );
		assertEquals( endDate, "2012030801" );
	}
	@Test 
	public void getEndDateString30Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String endDate = DateConverter.getEndDateString( 30, 1333155600000l );
		String endDate = DateConverter.getEndDateString( 30, 1333155600000l + 21600000l );
		assertEquals( endDate, "2012033101" );
	}
	@Test 
	public void getEndDateString60Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String endDate = DateConverter.getEndDateString( 60, 1333155600000l );
		String endDate = DateConverter.getEndDateString( 60, 1333155600000l + 21600000l );
		assertEquals( endDate, "20120331" );
	}
	@Test 
	public void getEndDateString90Days() {
		// timestamp is Human time (GMT): Sat, 31 Mar 2012 01:00:00 GMT
		//String endDate = DateConverter.getEndDateString( 90, 1333155600000l );
		String endDate = DateConverter.getEndDateString( 90, 1333155600000l + 21600000l );
		assertEquals( endDate, "20120331" );
	}
	
	@Test
	public void getDateStringsForRange() {
		List<String> dateStrings = DateConverter.getDateStringsForRange( "2012040100", "2012040123", 1 );
		assertEquals( dateStrings.size(), 24 );
		assertEquals( dateStrings.get(0), "2012040100" );
		assertEquals( dateStrings.get(23), "2012040123" );
	}
}
