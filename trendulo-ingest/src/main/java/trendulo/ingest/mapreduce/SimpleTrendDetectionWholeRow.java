package trendulo.ingest.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * jared@LinWorkstation:/trendulo/accumulo-1.4.0-incubating-SNAPSHOT/bin$ ./tool.sh /home/jared/repo/trendulo/trendulo-ingest/target/trendulo-ingest-0.1-SNAPSHOT.jar trendulo.ingest.mapreduce.SimpleTrendDetectionWholeRow
 */
public class SimpleTrendDetectionWholeRow extends Configured implements Tool {
	
	//public static class Map extends Mapper<Key,Value,Text,FloatWritable> {
	public static class Map extends Mapper<Key,Value,Text,Mutation> {
		
		private Value emptyValue = new Value( new byte[0] );
		
		/**
		 * Generate a trending score based on the counts for two time periods 
		 * @param counts the counters for two time periods
		 * @param context the Context containing properties for the total ngrams for the time periods
		 * @return the trending score or 0 if there is not enough data to generate a meaningful score
		 */
		private int calculateTrendingScore( long[] counts, Context context ) {
			int score = 0;
			
			// Pull the total count of all ngrams for each of the two time periods
			long totalCountTime0 = Long.parseLong( context.getConfiguration().get("total0") );
			long totalCountTime1 = Long.parseLong( context.getConfiguration().get("total1") );

			// Calculate the percentage of all ngrams our counters represent
			float percentTime0 = counts[0] / (float) totalCountTime0;
			float percentTime1 = counts[1] / (float) totalCountTime1;
			
			// If the second time period doesn't have a "sufficient" count, we won't even
			// bother generating a score to store. this will save space by helping ignore
			// ngrams that have too low counts to be "trending"
			if ( percentTime1 > .00001 ) {
				// multiply by 100 to shift the decimal before casting
				score = (int)((percentTime1 / percentTime0) * 100f);
			}
			
			return score;
		}
		
		@Override
		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {

			// As we set the WholeRowIterator, each value is going to be a serialized row 
			// so we now need to deserialize the value back into the Entrys for the row
			SortedMap<Key,Value> entries = WholeRowIterator.decodeRow(key, value);
			
			// If we have counts for the given ngram for both time periods, we will have 2 Entrys that look like
			// RowId: ngram
			// CQ: Date Granularity (ex. DAY, HOUR)
			// CF: Date Value (ex. 20120102, 2012010221)
			// Value: count
			// We know the entries are sorted by Key, so the first entry will have the earlier date value 
			if ( entries.size() == 2 ) {
				
				// Read the Entrys and pull out the two counts
				long [] counts = new long[2];
				int index = 0;
				for ( Entry<Key,Value> entry : entries.entrySet() ) {
					counts[index++] = Value.bytesToLong(entry.getValue().get());
				}
				
				// Generate a trending score for this ngram
				int score = calculateTrendingScore( counts, context );
				
				// If we have a valid score, write out the score to Accumulo
				if ( score > 0 ) {
					
					int sortableScore = Integer.MAX_VALUE - score;
					
					// RowId is the time we are calculating trends for, i.e. DAY:20120102
					// CF is Integer.MAX_VALUE - trending score so that we sort the highest scores first
					// CQ is the ngram
					// Value is empty
					String rowId = context.getConfiguration().get("rowId");
					Mutation mutation = new Mutation( rowId );
					mutation.put( new Text( String.valueOf(sortableScore)), entries.firstKey().getRow(), emptyValue );
					context.write( null, mutation );
				}
			}
		}   
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run( CachedConfiguration.getInstance(), new SimpleTrendDetectionWholeRow(), args );
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		String instanceName = "trendulo";
		String zookeepers = "192.168.1.101";
		String username = "root";
		String password = "secret";
		String inputTable = "tweets";
		String outputTable = "trends";
		String timeGranularity = "HOUR";
		String timeValue0 = "2012010400";
		String timeValue1 = "2012010501";
		
		Instance instance = new ZooKeeperInstance( instanceName, zookeepers);
		Connector connector = instance.getConnector( username, password);
		Scanner scanner = connector.createScanner( inputTable, new Authorizations());
		scanner.fetchColumn(new Text(timeGranularity),new Text(timeValue0));
		scanner.fetchColumn(new Text(timeGranularity),new Text(timeValue1));
		scanner.setRange( new Range("!NGRAMS") );
		long totals[] = new long[2];
		int index = 0;
		for ( Entry<Key,Value> entry : scanner ) {
			totals[index++] = Value.bytesToLong( entry.getValue().get() );
		}
		
		Job job = new Job( getConf(), SimpleTrendDetectionWholeRow.class.getName() );
		job.setJarByClass( this.getClass() );
		job.setInputFormatClass( AccumuloInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		System.out.println("Totals[0]:" + totals[0]);
		System.out.println("Totals[1]:" + totals[1]);
		job.getConfiguration().set("total0", String.valueOf(totals[0]));
		job.getConfiguration().set("total1", String.valueOf(totals[1]));
		job.getConfiguration().set("rowId", timeGranularity + ":" + timeValue1);
		
		// Configure InputFormat
		AccumuloInputFormat.setInputInfo( job.getConfiguration(), username, password.getBytes(), inputTable, new Authorizations() );
		AccumuloInputFormat.setZooKeeperInstance( job.getConfiguration(), instanceName, zookeepers );
		ArrayList<Pair<Text,Text>> columns = new ArrayList<Pair<Text,Text>>();
		columns.add( new Pair<Text,Text>(new Text(timeGranularity),new Text(timeValue0)));
		columns.add( new Pair<Text,Text>(new Text(timeGranularity),new Text(timeValue1)));
		AccumuloInputFormat.fetchColumns( job.getConfiguration(), columns );
		
		// Set the WholeRowIterator
		IteratorSetting iteratorSetting = new IteratorSetting( 30, WholeRowIterator.class );
		AccumuloInputFormat.addIterator(job.getConfiguration(), iteratorSetting );
		
		ArrayList<Range> ranges = new ArrayList<Range>();
		// limit ranges for testing to make job run quickly
		//ranges.add( new Range( new Text("b"), new Text("c") ) );
		//AccumuloInputFormat.setRanges( job.getConfiguration(), ranges);
		
		// Configure OutputFormat
		/*
		TextOutputFormat.setOutputPath(job, new Path( outputPath ) );
		//job.setOutputKeyClass( Text.class );
		//job.setOutputValueClass( FloatWritable.class );
		job.setOutputKeyClass( FloatWritable.class );
		job.setOutputValueClass( Text.class );
		*/
		
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, outputTable );
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), instanceName, zookeepers );
		    
		
		job.setMapperClass( Map.class );
	    job.setNumReduceTasks(0);
		
		job.waitForCompletion( true );
		
		return 0;
	}

}
