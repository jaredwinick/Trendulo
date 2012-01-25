package trendulo.ingest;

import org.apache.log4j.Logger;

public class TemporalNGramIngester implements Runnable {

	private TemporalNGramSource temporalNGramSource;
	private boolean shutdown = false;
	private long nGramsIngested = 0;
	
	private Logger log = Logger.getLogger( TemporalNGramIngester.class );
	
	public TemporalNGramIngester( TemporalNGramSource temporalNGramSource ) {
		this.temporalNGramSource = temporalNGramSource;
	}
	
	public void run() {
		while ( shutdown == false ) {
			TemporalNGram temporalNGram = null;
			try {
				temporalNGram = temporalNGramSource.take();
				++nGramsIngested;
			}
			catch ( InterruptedException e ) {
				log.error( "Interrupted while waiting on take()", e );
				continue;
			}
			
			if ( nGramsIngested % 10000 == 0 ) {
				log.debug( "Number of N-Grams Ingested: " + nGramsIngested );
			}
		}
	}
	
	/**
	 * Request the thread to shutdown by exiting the while loop of the run() method
	 */
	public void shutdown() {
		this.shutdown = true;
	}

}
