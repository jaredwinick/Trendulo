package trendulo.ingest;

public class StringUtilities {

	/**
	 * Remove characters from the string sequence that will cause problems with our identification of n-grams
	 * @param stringSequence The string to be cleaned up
	 * @return The cleaned string with characters removed
	 */
	public static String cleanStringSequence( String stringSequence ) {
		return stringSequence.replaceAll( "\\.|,|!|\\?|\\(|\\)|\\r|\\n", "" ).toLowerCase().trim().replaceAll(" +", " ");
	}
}
