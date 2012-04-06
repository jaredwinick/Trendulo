package trendulo.ingest;

import java.util.List;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Required;

public class TemporalNGramIngester implements Runnable {

	private TemporalNGramSource temporalNGramSource;
	private boolean shutdown = false;
	private long nGramsIngested = 0;
	
	private ZooKeeperInstance instance;
	private Connector connector;
	private BatchWriter batchWriter;
	
	private String nGramCounterRowId; 
	private String totalSequencesRowId;
	
	private long gmtOffset = 0;

	private Logger log = Logger.getLogger( TemporalNGramIngester.class );
	
	public TemporalNGramIngester( TemporalNGramSource temporalNGramSource, ZooKeeperInstance instance, Connector connector, BatchWriter batchWriter ) {
		this.temporalNGramSource = temporalNGramSource;
		this.instance = instance;
		this.connector = connector;
		this.batchWriter = batchWriter;
	}
	
	public void run() {
		while ( shutdown == false ) {
			List<TemporalNGram> temporalNGrams = null;
			try {
				temporalNGrams = temporalNGramSource.take();
				for ( TemporalNGram temporalNGram : temporalNGrams ) {
					// Adjust the timestamp if the user has specified an offset
					temporalNGram.setTimestamp( temporalNGram.getTimestamp() + gmtOffset );
					
					Mutation mutation = MutationGenerator.generateMutationVLen( temporalNGram );
					batchWriter.addMutation( mutation );
					
					++nGramsIngested;
					if ( nGramsIngested % 10000 == 0 ) {
						log.debug( "Number of N-Grams Ingested: " + nGramsIngested + " offset" + gmtOffset);
					}
				}
				// Get the timestamp for these n-grams. It will be the same for each one in the List
				long timestamp = 0;
				if ( temporalNGrams.size() > 0 ) {
					timestamp = temporalNGrams.get(0).getTimestamp();
				}
				// update the total ngrams counter - obviously not the smartest way as this row is going to be hot
				Mutation nGramCounterMutation = MutationGenerator.generateMutationVLen( new TemporalNGram( nGramCounterRowId, timestamp ), (long)temporalNGrams.size() );
				batchWriter.addMutation( nGramCounterMutation );
				
				// update the total sequences counter by 1
				Mutation totalSequencesMutation = MutationGenerator.generateMutationVLen( new TemporalNGram( totalSequencesRowId, timestamp ) );
				batchWriter.addMutation( totalSequencesMutation );
			}
			catch ( InterruptedException e ) {
				log.error( "Interrupted while waiting on take()", e );
			} catch (MutationsRejectedException e) {
				log.error( "Error writing Mutation", e );
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

	public String getnGramCounterRowId() {
		return nGramCounterRowId;
	}

	@Required
	public void setnGramCounterRowId(String nGramCounterRowId) {
		this.nGramCounterRowId = nGramCounterRowId;
	}

	public String getTotalSequencesRowId() {
		return totalSequencesRowId;
	}

	@Required
	public void setTotalSequencesRowId(String totalSequencesRowId) {
		this.totalSequencesRowId = totalSequencesRowId;
	}

	public long getGmtOffset() {
		return gmtOffset;
	}

	public void setGmtOffset(long gmtOffset) {
		this.gmtOffset = gmtOffset;
	}

}
