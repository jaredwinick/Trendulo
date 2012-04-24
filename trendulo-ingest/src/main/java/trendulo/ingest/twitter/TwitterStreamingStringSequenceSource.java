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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import trendulo.ingest.TemporalStringSequence;
import trendulo.ingest.TemporalStringSequenceSource;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.BasicAuthorization;
import twitter4j.json.DataObjectFactory;

public class TwitterStreamingStringSequenceSource implements
		TemporalStringSequenceSource, StatusListener {
	
	private BlockingQueue<Status> statusQueue;
	private Authorization auth;
	private TwitterStream twitterStream;
	private OutputStream archiveOutputStream;
	private StatusFilter statusFilter;
	
	private Logger log = Logger.getLogger( TwitterStreamingStringSequenceSource.class );
	public final static int DEFAULT_BUFFER_SIZE = 10000;
	
	public TwitterStreamingStringSequenceSource( String username, String password ) {
		this( username, password, null, null );
	}
	public TwitterStreamingStringSequenceSource( String username, String password, String archiveTwitterFilePath ) {
		this( username, password, archiveTwitterFilePath, null );
	}
	public TwitterStreamingStringSequenceSource( String username, String password, String archiveTwitterFilePath, StatusFilter statusFilter ) {
		
		this.statusFilter = statusFilter;
		statusQueue = new ArrayBlockingQueue<Status>( DEFAULT_BUFFER_SIZE );
		
		// If the user wants status messages archived, create an OutputStream
		if ( archiveTwitterFilePath != null ) {
			try {
				// Verify that the file doesn't already exist so we don't clobber the archive
				if ( new File( archiveTwitterFilePath).exists() ) {
					throw new RuntimeException( "Archive File Already Exists. Please move/rename before starting Ingest: " + archiveTwitterFilePath );
				}
				
				FileOutputStream fos = new FileOutputStream( archiveTwitterFilePath );
				if ( archiveTwitterFilePath.endsWith("gz") ) {
					archiveOutputStream = new GZIPOutputStream( new BufferedOutputStream( fos ) );
				} else {
					archiveOutputStream = fos;
				}
			} catch ( IOException e ) {
				log.error( "Error creating archive file", e );
			}
		}
		
		// Configure the Twitter Streaming API client
		
		// this system property is required to access raw json
		System.setProperty("twitter4j.jsonStoreEnabled", "true");
		
		auth = new BasicAuthorization( username, password );
		twitterStream = new TwitterStreamFactory().getInstance( auth );
		twitterStream.addListener( this );
		
		// We only want status messages with a geo. This needs to be pulled
		// out so it is configurable
		// Right now just the lower 48 of the US
		FilterQuery filterQuery = new FilterQuery();
		double [][] locations = {{-126.0,24.0},{-67.0,49.0}};
		filterQuery.locations(locations);
		twitterStream.filter( filterQuery );
	}
	
	public void shutdown() {
		twitterStream.cleanUp();
		if ( archiveOutputStream != null ) {
			try {
				archiveOutputStream.close();
			} catch (IOException e) {
				log.error( "Error cleaning up OutputStream", e );
			}
		}
	}

	/**
	 * Needs work, copied from TwitterFileStringSequenceSource and running out of time
	 */
	@Override
	public TemporalStringSequence nextStringSequence() {
		TemporalStringSequence temporalStringSequence = null;
		Status status = null;
		boolean statusAccepted = false;
		
		// Pull a status off the queue. Block until available
		do {
			try {
				status = statusQueue.take();
			} catch (InterruptedException e) {
				log.error(e);
			}
			if (status != null) {
				// if the user has specified a StatusFilter, check to see if the
				// Status object
				// is accepted. If not, we will continue around the loop
				if (statusFilter != null) {
					if (statusFilter.accept(status)) {
						statusAccepted = true;
					}
				}
				// Every status is accepted if there is no filter
				else {
					statusAccepted = true;
				}
			}
		} while (status != null && statusAccepted == false);

		if (status != null && statusAccepted == true) {
			temporalStringSequence = new TemporalStringSequence(
					status.getText(), status.getCreatedAt().getTime());
		}

		return temporalStringSequence;
	}
	
	@Override
	public void onException(Exception ex) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void onStatus(Status status) {
		// Save the status to disk if the user has specified an archive file
		if ( archiveOutputStream != null ) {
			archiveStatus( status );
		}
		
		// Put the message on the status queue if there is room
		statusQueue.offer( status );
	}
	
	private void archiveStatus( Status status ) {
		 String json = DataObjectFactory.getRawJSON( status );
		 log.trace( json );
		 
		 try {
			 archiveOutputStream.write( json.getBytes() );
			 archiveOutputStream.write( "\n".getBytes() );
		} catch (IOException e) {
			log.error( "Error writing to archive", e );
		}
	}
	
	@Override
	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		log.info( "LIMITED STATUSES: " + numberOfLimitedStatuses );
		
	}
	@Override
	public void onScrubGeo(long userId, long upToStatusId) {
		// TODO Auto-generated method stub
		
	}
}
