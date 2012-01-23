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

import java.util.List;

import org.junit.Test;

public class NGramGeneratorTest {
	
	@Test
	public void testGenerateNGramsNEquals1() {
		List<String> nGrams = NGramGenerator.generateNGrams( "the dog is brown", 1 );
		
		assertEquals( nGrams.size(), 4 );
		assertTrue( nGrams.contains( "the" ) );
		assertTrue( nGrams.contains( "dog" ) );
		assertTrue( nGrams.contains( "is" ) );
		assertTrue( nGrams.contains( "brown" ) );
	}
	
	@Test
	public void testGenerateNGramsNEquals2() {

		List<String> nGrams = NGramGenerator.generateNGrams( "the dog is brown", 2 );
		
		assertEquals( nGrams.size(), 3 );
		assertTrue( nGrams.contains( "the dog" ) );
		assertTrue( nGrams.contains( "dog is" ) );
		assertTrue( nGrams.contains( "is brown" ) );
	}
	
	@Test
	public void testGenerateNGramsNEquals3() {

		List<String> nGrams = NGramGenerator.generateNGrams( "the dog is brown", 3 );
		
		assertEquals( nGrams.size(), 2 );
		assertTrue( nGrams.contains( "the dog is" ) );
		assertTrue( nGrams.contains( "dog is brown" ) );
	}
	
	@Test
	public void testGenerateNGramsNEquals4() {

		List<String> nGrams = NGramGenerator.generateNGrams( "the dog is brown", 4 );
		
		assertEquals( nGrams.size(), 1 );
		assertTrue( nGrams.contains( "the dog is brown" ) );
	}
	
	@Test
	public void testGenerateNGramsNEquals5() {

		List<String> nGrams = NGramGenerator.generateNGrams( "the dog is brown", 5 );
		
		assertEquals( nGrams.size(), 0 );
	}
	
	@Test
	public void testGenerateAllNGramsInRange1to5() {
		
		List<String> nGrams = NGramGenerator.generateAllNGramsInRange( "the dog is brown", 1, 5 );
		assertEquals( nGrams.size(), 10 );
		assertTrue( nGrams.contains( "the" ) );
		assertTrue( nGrams.contains( "dog" ) );
		assertTrue( nGrams.contains( "is" ) );
		assertTrue( nGrams.contains( "brown" ) );
		assertTrue( nGrams.contains( "the dog" ) );
		assertTrue( nGrams.contains( "dog is" ) );
		assertTrue( nGrams.contains( "is brown" ) );
		assertTrue( nGrams.contains( "the dog is" ) );
		assertTrue( nGrams.contains( "dog is brown" ) );
		assertTrue( nGrams.contains( "the dog is brown" ) );
	
	}
}
