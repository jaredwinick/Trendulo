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

public class TemporalStringSequence {
	
	private String stringSequence;
	private long timestamp;
	
	public TemporalStringSequence() { }

	public TemporalStringSequence(String stringSequence, long timestamp) {
		this.stringSequence = stringSequence;
		this.timestamp = timestamp;
	}
	
	public String getStringSequence() {
		return stringSequence;
	}
	public void setStringSequence(String stringSequence) {
		this.stringSequence = stringSequence;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
}
