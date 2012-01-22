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

public class TemporalNGram {

	// the n-gram string
	private String 	nGram;
	// UNIX timestamp in GMT
	private long 	timestamp;
	
	public TemporalNGram() { }
	
	public TemporalNGram( String nGram, long timestamp ) {
		this.nGram = nGram;
		this.timestamp = timestamp;
	}
	
	public String getnGram() {
		return nGram;
	}
	public void setnGram(String nGram) {
		this.nGram = nGram;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}	
}
