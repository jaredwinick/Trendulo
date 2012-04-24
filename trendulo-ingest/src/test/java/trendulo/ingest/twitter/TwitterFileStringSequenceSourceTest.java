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


package trendulo.ingest.twitter;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Test;

import trendulo.ingest.TemporalStringSequence;
import trendulo.ingest.TemporalStringSequenceSource;

public class TwitterFileStringSequenceSourceTest {
	
	@Test
	public void testPlainTextFileFileSystem() throws IOException {
		
		TemporalStringSequenceSource source = null;
		source = new TwitterFileStringSequenceSource( "src/test/resources/sampleStatusMessages.json" );
		
		TemporalStringSequence status = null;
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "Diffused sunlight #BlogRefresh http://t.co/TDyAk9qY" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@vicegandako i really love ur shows! Thank u very very much for making my every sunday night an awesome one! Love u vice... God bless" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@wired I keep complaining to government that the windfarms are poluting the sea with noise as sound travles further underwater in sea n sand" );
		status = source.nextStringSequence();
		assertEquals( status, null );
	}
	
	@Test
	public void testPlainTextFileFileSystemStatusFilter() throws IOException {
		
		TemporalStringSequenceSource source = null;
		source = new TwitterFileStringSequenceSource( "src/test/resources/sampleStatusMessages.json", new USCountryCodeStatusFilter() );
		
		TemporalStringSequence status = null;
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "Diffused sunlight #BlogRefresh http://t.co/TDyAk9qY" );
		status = source.nextStringSequence();
		assertEquals( status, null );
	}
	
	@Test
	public void testPlainTextFileClasspath() throws IOException {
		
		TemporalStringSequenceSource source = null;
		source = new TwitterFileStringSequenceSource( "sampleStatusMessages.json" );
		
		TemporalStringSequence status = null;
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "Diffused sunlight #BlogRefresh http://t.co/TDyAk9qY" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@vicegandako i really love ur shows! Thank u very very much for making my every sunday night an awesome one! Love u vice... God bless" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@wired I keep complaining to government that the windfarms are poluting the sea with noise as sound travles further underwater in sea n sand" );
		status = source.nextStringSequence();
		assertEquals( status, null );
	}
	
	@Test
	public void testGZipFileClasspath() throws IOException {
		
		TemporalStringSequenceSource source = null;
		source = new TwitterFileStringSequenceSource( "sampleStatusMessages.json.gz" );
		
		TemporalStringSequence status = null;
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "Diffused sunlight #BlogRefresh http://t.co/TDyAk9qY" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@vicegandako i really love ur shows! Thank u very very much for making my every sunday night an awesome one! Love u vice... God bless" );
		status = source.nextStringSequence();
		assertEquals( status.getStringSequence(), "@wired I keep complaining to government that the windfarms are poluting the sea with noise as sound travles further underwater in sea n sand" );
		status = source.nextStringSequence();
		assertEquals( status, null );
	}
	
	
	@Test( expected=FileNotFoundException.class )
	public void testPlainTextFileFileSystemNotFound() throws IOException {
		
		TemporalStringSequenceSource source = null;
		source = new TwitterFileStringSequenceSource( "src/test/resources/sampleStatusMessagesNotAFile.json" );
	}
		
	

}
