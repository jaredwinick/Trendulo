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


package trendulo.ingest.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DayCount implements WritableComparable {
	public long count;
	public int day;
	
	public DayCount() {}
	public DayCount(DayCount other) {
		this.count = other.count;
		this.day = other.day;
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(count);
		out.writeInt(day);
	}
	public void readFields(DataInput in) throws IOException {
		count = in.readLong();
		day = in.readInt();
	}
	public int compareTo(Object o) {
		DayCount other = (DayCount)o;
		return (day - other.day);
	}
	public boolean equals(Object o) {
		if (!(o instanceof DayCount)) {
			return false;
		}
		DayCount other = (DayCount)o;
		return (day == other.day);
	}
	public int hashCode() {
		return day;
	}
	
	@Override
	public String toString() {
		return String.format("DAY: %d  COUNT: %d", day, count);
	}
}