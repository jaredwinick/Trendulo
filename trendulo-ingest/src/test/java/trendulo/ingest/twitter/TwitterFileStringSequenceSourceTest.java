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
