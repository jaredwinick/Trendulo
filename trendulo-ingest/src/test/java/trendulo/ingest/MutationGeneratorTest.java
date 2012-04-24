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


package trendulo.ingest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
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
		assertEquals( mutation.getUpdates().size(), 2 );
		/* Decided we don't need to keep month info around (JDW - 03-24-2012)
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "MONTH".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "201201".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), Value.longToBytes(1) ) );
		*/
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "DAY".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "20120125".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), Value.longToBytes(1) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnFamily(), "HOUR".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnQualifier(), "2012012505".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getValue(), Value.longToBytes(1) ) );
	}
	
	@Test
	public void testGenerateMutationWithCount() throws IOException {
		TemporalNGram temporalNGram = new TemporalNGram( "trendulo is great", 1327470661000L );
		Mutation mutation = MutationGenerator.generateMutation( temporalNGram, 36l );
		assertTrue( Arrays.equals( mutation.getRow(), "trendulo is great".getBytes() ) );
		assertEquals( mutation.getUpdates().size(), 2 );

		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "DAY".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "20120125".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), Value.longToBytes(36) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnFamily(), "HOUR".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnQualifier(), "2012012505".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getValue(), Value.longToBytes(36) ) );
	}
	
	@Test
	public void testGenerateMutationVLen() throws IOException {
		TemporalNGram temporalNGram = new TemporalNGram( "trendulo is great", 1327470661000L );
		Mutation mutation = MutationGenerator.generateMutationVLen( temporalNGram );
		assertTrue( Arrays.equals( mutation.getRow(), "trendulo is great".getBytes() ) );
		assertEquals( mutation.getUpdates().size(), 2 );

		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "DAY".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "20120125".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), LongCombiner.VAR_LEN_ENCODER.encode(1l) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnFamily(), "HOUR".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnQualifier(), "2012012505".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getValue(), LongCombiner.VAR_LEN_ENCODER.encode(1l) ) );
	}
	
	@Test
	public void testGenerateMutationVLenWithCount() throws IOException {
		
		Long longNumber = 36363636l;
		TemporalNGram temporalNGram = new TemporalNGram( "trendulo is great", 1327470661000L );
		Mutation mutation = MutationGenerator.generateMutationVLen( temporalNGram, longNumber );
		assertTrue( Arrays.equals( mutation.getRow(), "trendulo is great".getBytes() ) );
		assertEquals( mutation.getUpdates().size(), 2 );

		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnFamily(), "DAY".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getColumnQualifier(), "20120125".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(0).getValue(), LongCombiner.VAR_LEN_ENCODER.encode(longNumber) ) );
		
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnFamily(), "HOUR".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getColumnQualifier(), "2012012505".getBytes() ) );
		assertTrue( Arrays.equals( mutation.getUpdates().get(1).getValue(), LongCombiner.VAR_LEN_ENCODER.encode(longNumber) ) );
	}

}
