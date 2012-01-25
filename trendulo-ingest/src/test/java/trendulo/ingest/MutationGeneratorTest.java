package trendulo.ingest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;

public class MutationGeneratorTest {

	@Test
	public void testTimestampToMonthString() {
		assertEquals( MutationGenerator.timestampToMonthString( 1327470661000L ), "201201" ); 
	}
	
	@Test
	public void testTimestampToDayString() {
		assertEquals( MutationGenerator.timestampToDayString( 1327470661000L ), "20120125" ); 
	}
	
	@Test
	public void testTimestampToHourString() {
		assertEquals( MutationGenerator.timestampToHourString( 1327470661000L ), "2012012505" ); 
	}
	
	@Test
	public void testGenerateMutation() throws IOException {
		TemporalNGram temporalNGram = new TemporalNGram( "trendulo is great", 1327470661000L );
		Mutation mutation = MutationGenerator.generateMutation( temporalNGram );
		assertTrue( Arrays.equals( mutation.getRow(), "trendulo is great".getBytes() ) );
		assertEquals( mutation.getUpdates().size(), 3 );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "MONTH".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "201201".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), Value.longToBytes(1) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnFamily(), "DAY".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnQualifier(), "20120125".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getValue(), Value.longToBytes(1) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(2).getColumnFamily(), "HOUR".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(2).getColumnQualifier(), "2012012505".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(2).getValue(), Value.longToBytes(1) ) );
	}

}
