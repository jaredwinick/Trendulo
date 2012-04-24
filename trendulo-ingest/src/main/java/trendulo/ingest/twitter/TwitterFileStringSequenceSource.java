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
	private StatusFilter statusFilter;
	private Logger log = Logger.getLogger( TwitterFileStringSequenceSource.class );
	
	public TwitterFileStringSequenceSource( String twitterFilePath ) throws IOException {
		this( twitterFilePath, null );
	}
	
	/**
	 * Creates a new TwitterFileStringSequenceSource. This class reads a file that has been 
	 * created by writing the stream of JSON strings from the Twitter Streaming API. 
	 * @param twitterFilePath The full filesystem path or path on classpath to the file of JSON-serialized Twitter Statuses. 
	 * 	This file can be plain-text or GZIPed (designated by a .gz extension)
	 * @param statusFilter a StatusFilter to accept/reject Status messages
	 * @throws IOException 
	 */
	public TwitterFileStringSequenceSource( String twitterFilePath, StatusFilter statusFilter ) throws IOException {
		this.twitterFilePath = twitterFilePath;
		this.statusFilter = statusFilter;
		
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
		log.debug("Opening file: " + twitterFilePath );
		bufferedReader = new BufferedReader( new InputStreamReader( inputStream ) );
		
		System.setProperty("twitter4j.jsonStoreEnabled", "true");
	}
	
	public TemporalStringSequence nextStringSequence() {
		
		TemporalStringSequence temporalStringSequence = null;
		String line = null;
		Status status = null;
		boolean statusAccepted = false;
		
		// Read a line from the file and serialize the JSON string into a Status object
		try {
			do {
				line = bufferedReader.readLine( );
				if ( line != null ) {
					status = DataObjectFactory.createStatus( line );
					// if the user has specified a StatusFilter, check to see if the Status object
					// is accepted. If not, we will continue around the loop
					if ( statusFilter != null ) {
						if ( statusFilter.accept( status ) ) {
							statusAccepted = true;
						}
					}
					// Every status is accepted if there is no filter
					else {
						statusAccepted = true;
					}
				}
			} while ( line != null && statusAccepted == false );
				
		} catch (IOException e) {
			log.error( "Error reading from file: " + twitterFilePath, e );
		} catch (TwitterException e) {
			log.error( "Error parsing JSON status string: " + line, e );
		}
		
		if ( status != null && statusAccepted == true ) {
			temporalStringSequence = new TemporalStringSequence( status.getText(), status.getCreatedAt().getTime() );
		}
		
		return temporalStringSequence;
	}

}
