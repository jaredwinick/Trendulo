package trendulo.web.model;

import java.util.List;

public class Trends {

	public class Trend {
		private String ngram;
		private int score;
		public Trend() {}
		public Trend( String ngram, int score) {
			this.ngram = ngram;
			this.score = score;
		}
		public String getNgram() {
			return ngram;
		}
		public void setNgram(String ngram) {
			this.ngram = ngram;
		}
		public int getScore() {
			return score;
		}
		public void setScore(int score) {
			this.score = score;
		}
	}
	private String dateString;
	private List<Trend> trends;
	public String getDateString() {
		return dateString;
	}
	public void setDateString(String dateString) {
		this.dateString = dateString;
	}
	public List<Trend> getTrends() {
		return trends;
	}
	public void setTrends(List<Trend> trends) {
		this.trends = trends;
	}
}
