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

import java.util.ArrayList;
import java.util.List;

/**
 * N-Gram generation. Source is borrowed from @aioobe's answer at 
 * http://stackoverflow.com/questions/3656762/n-gram-generation-from-a-sentence
 *
 */
public class NGramGenerator {
	
	public static List<String> generateNGrams( String sourceString, int n ) {
		
		List<String> nGrams = new ArrayList<String>( );
		String[] words = sourceString.split( " " );
		for ( int i = 0; i < words.length - n + 1; ++i ) {
			nGrams.add( concat( words, i, i + n ) );
		}
		
		return nGrams;
	}
	
	public static List<String> generateAllNGramsInRange( String sourceString, int nStart, int nEnd ) {
		
		List<String> nGrams = new ArrayList<String>( );
		for ( int currentN = nStart; currentN <= nEnd; ++currentN ) {
			nGrams.addAll( generateNGrams( sourceString, currentN ) );
		}
		
		return nGrams;
	}
	
	private static String concat( String[] words, int start, int end ) {
		
		StringBuilder sb = new StringBuilder();
		for ( int i = start; i < end; ++i ) {
			sb.append( ( i > start ? " " : "" ) + words[i] );
		}
		return sb.toString();
	}
}
