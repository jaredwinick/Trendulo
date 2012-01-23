package trendulo.ingest.twitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import trendulo.ingest.TemporalStringSequence;
import trendulo.ingest.TemporalStringSequenceSource;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.internal.logging.Logger;
import twitter4j.json.DataObjectFactory;

public class TwitterFileStringSequenceSource implements TemporalStringSequenceSource {

	private String twitterFilePath;
	private BufferedReader bufferedReader;
	private Logger log = Logger.getLogger( TwitterFileStringSequenceSource.class );
	
	/**
	 * Creates a new TwitterFileStringSequenceSource. This class reads a file that has been 
	 * created by writing the stream of JSON strings from the Twitter Streaming API. 
	 * @param twitterFilePath The full filesystem path or path on classpath to the file of JSON-serialized Twitter Statuses. 
	 * 	This file can be plain-text or GZIPed (designated by a .gz extension)
	 * @throws IOException 
	 */
	public TwitterFileStringSequenceSource( String twitterFilePath ) throws IOException {
		this.twitterFilePath = twitterFilePath;
		
		InputStream inputStream = null;
		
		// Check if the file existing on the file system at the specified path
		try {
			inputStream = new FileInputStream( twitterFilePath );
		}
		catch ( FileNotFoundException e ) {
			log.debug( String.format("Absolute path [%s] not found. Trying classpath...", twitterFilePath ) );
			inputStream = getClass().getClassLoader().getResourceAsStream( twitterFilePath );
		}
		
		// check to see if the inputStream is null. if so, the file doesn't exist
		if ( inputStream == null ) {
			throw new FileNotFoundException( String.format( "File [%s] not found on file system or classpath", twitterFilePath ) );
		}
		
		if ( twitterFilePath.endsWith( ".gz" ) ) {
			inputStream = new GZIPInputStream( inputStream );
		}
		bufferedReader = new BufferedReader( new InputStreamReader( inputStream ) );
		
		System.setProperty("twitter4j.jsonStoreEnabled", "true");
	}
	
	public TemporalStringSequence nextStringSequence() {
		
		TemporalStringSequence temporalStringSequence = null;
		String line = null;
		Status status = null;
		
		// Read a line from the file and serialize the JSON string into a Status object
		try {
			line = bufferedReader.readLine( );
			if ( line != null ) {
				status = DataObjectFactory.createStatus( line );
			}
		} catch (IOException e) {
			log.error( "Error reading from file: " + twitterFilePath, e );
		} catch (TwitterException e) {
			log.error( "Error parsing JSON status string: " + line, e );
		}
		
		if ( status != null ) {
			temporalStringSequence = new TemporalStringSequence( status.getText(), status.getCreatedAt().getTime() );
		}
		
		return temporalStringSequence;
	}

}
