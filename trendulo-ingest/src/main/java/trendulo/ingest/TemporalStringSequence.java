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
