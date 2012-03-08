package trendulo.ingest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MutationGenerator {

	private static Logger log = Logger.getLogger( MutationGenerator.class );
	
	/**
	 * Generate a list of Accumulo Mutations for the TemporalNGram. The n-gram string is
	 * the row. CFs represent the different time granularities we are keeping counters for.
	 * CQs represent the time granularity value.
	 * @param temporalNGram The TemporalNGram to generate mutations for
	 * @return
	 */
	public static Mutation generateMutation( TemporalNGram temporalNGram ) {
		
		Mutation mutation = new Mutation( new Text( temporalNGram.getnGram() ) );
		
		try {
			mutation.put( new Text( "MONTH" ), new Text( timestampToMonthString( temporalNGram.getTimestamp() ) ), new Value( Value.longToBytes( 1 ) ) );
			mutation.put( new Text( "DAY" ), new Text( timestampToDayString( temporalNGram.getTimestamp() ) ), new Value( Value.longToBytes( 1 ) ) );
			mutation.put( new Text( "HOUR" ), new Text( timestampToHourString( temporalNGram.getTimestamp() ) ), new Value( Value.longToBytes( 1 ) ) );
		} catch (IOException e) {
			log.error( e );
		}
		
		return mutation;
	}
	
	public static String timestampToMonthString( long timestamp ) {
		DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		return String.format( "%d%02d", dateTime.getYear(), dateTime.getMonthOfYear() );
	}
	
	public static String timestampToDayString( long timestamp ) {
		DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		return String.format( "%d%02d%02d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth() );
	}
	
	public static String timestampToHourString( long timestamp ) {
		DateTime dateTime = new DateTime( timestamp, DateTimeZone.UTC );
		return String.format( "%d%02d%02d%02d", dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), dateTime.getHourOfDay() );
	}
	
	
}
