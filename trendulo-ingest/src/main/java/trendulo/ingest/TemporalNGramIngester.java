package trendulo.ingest;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class TemporalNGramIngester implements Runnable {

	private TemporalNGramSource temporalNGramSource;
	private boolean shutdown = false;
	private long nGramsIngested = 0;
	
	private ZooKeeperInstance instance;
	private Connector connector;
	private BatchWriter batchWriter;
	
	private String nGramCounterRowId = "!NGRAMS";

	private Logger log = Logger.getLogger( TemporalNGramIngester.class );
	
	public TemporalNGramIngester( TemporalNGramSource temporalNGramSource, ZooKeeperInstance instance, Connector connector, BatchWriter batchWriter ) {
		this.temporalNGramSource = temporalNGramSource;
		this.instance = instance;
		this.connector = connector;
		this.batchWriter = batchWriter;
	}
	
	public void run() {
		while ( shutdown == false ) {
			TemporalNGram temporalNGram = null;
			try {
				temporalNGram = temporalNGramSource.take();
				Mutation mutation = MutationGenerator.generateMutation( temporalNGram );
				batchWriter.addMutation( mutation );
				++nGramsIngested;
				
				// update the total ngrams counter - obviously not the smartest way as this row is going to be hot
				Mutation nGramCounterMutation = MutationGenerator.generateMutation( new TemporalNGram( nGramCounterRowId, temporalNGram.getTimestamp() ) );
				batchWriter.addMutation( nGramCounterMutation );
			}
			catch ( InterruptedException e ) {
				log.error( "Interrupted while waiting on take()", e );
			} catch (MutationsRejectedException e) {
				log.error( "Error writing Mutation", e );
			}
			
			if ( nGramsIngested % 10000 == 0 ) {
				log.debug( "Number of N-Grams Ingested: " + nGramsIngested );
			}
		}
		
		// shutdown
		log.debug("Shutting Down");
		try {
			batchWriter.close();
		} catch (MutationsRejectedException e) {
			log.error( "Error writing Mutation", e );
		}
	}
	
	/**
	 * Request the thread to shutdown by exiting the while loop of the run() method
	 */
	public void shutdown() {
		this.shutdown = true;
	}

}
