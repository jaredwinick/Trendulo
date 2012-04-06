package trendulo.ingest.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleTrendDetectionWholeRow extends Configured implements Tool {
	
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;
	private static String USAGE = "$ACCUMULO_HOME/bin/tool.sh trendulo-ingest.jar trendulo.ingest.mapreduce.SimpleTrendDetectionWholeRow <instance name> <zoo keepers> <input table> <output table> <time granularity> <time value 0> <time value 1> [-u <username> -p password]";

	static {
		usernameOpt = new Option("u", "username", true, "username");
		passwordOpt = new Option("p", "password", true, "password");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}

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
			if ( percentTime1 > .00002 ) {
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
					counts[index++] = LongCombiner.VAR_LEN_ENCODER.decode( entry.getValue().get());
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

	public int run(String[] unprocessed_args) throws Exception {

		Parser p = new BasicParser();
		CommandLine cl = p.parse(opts, unprocessed_args);
		String[] args = cl.getArgs();
		String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length != 7) {
			System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 7.");
			return printUsage();
		}
		    
		String instanceName = args[0];
		String zookeepers = args[1];
		String inputTable = args[2];
		String outputTable = args[3];
		String timeGranularity = args[4];
		String timeValue0 = args[5];
		String timeValue1 = args[6];
		
		// Configure the MR job
		Job job = new Job( getConf(), SimpleTrendDetectionWholeRow.class.getName() );
		job.setJarByClass( this.getClass() );
		job.setInputFormatClass( AccumuloInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		// Get the total counts for the two time periods and put these values in the configuration so they 
		// can be accessed by the Mapper. Also add the rowId string to use for writing results back to Accumulo
		long totalCounts[]  = getTotalCounts( instanceName, zookeepers, username, password, inputTable, timeGranularity, timeValue0, timeValue1 );
		System.out.println("Totals[0]:" + totalCounts[0]);
		System.out.println("Totals[1]:" + totalCounts[1]);
		job.getConfiguration().set("total0", String.valueOf(totalCounts[0]));
		job.getConfiguration().set("total1", String.valueOf(totalCounts[1]));
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
		
		// limit ranges for testing to make job run quickly
		/*
		ArrayList<Range> ranges = new ArrayList<Range>();
		ranges.add( new Range( new Text("b"), new Text("c") ) );
		AccumuloInputFormat.setRanges( job.getConfiguration(), ranges);
		*/
		
		// Configure OutputFormat
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
	
	private long [] getTotalCounts( String instanceName, String zookeepers, String username, 
									String password, String inputTable, String timeGranularity, 
									String timeValue0, String timeValue1 ) 
						throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
		
		Instance instance = new ZooKeeperInstance( instanceName, zookeepers);
		Connector connector = instance.getConnector( username, password);
		Scanner scanner = connector.createScanner( inputTable, new Authorizations());
		scanner.fetchColumn(new Text(timeGranularity),new Text(timeValue0));
		scanner.fetchColumn(new Text(timeGranularity),new Text(timeValue1));
		scanner.setRange( new Range("!TWEETS") );
		long totalCounts[] = new long[2];
		int index = 0;
		for ( Entry<Key,Value> entry : scanner ) {
			totalCounts[index++] = LongCombiner.VAR_LEN_ENCODER.decode( entry.getValue().get() );
		}
		
		return totalCounts;
	}
	
	private int printUsage() {
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp(USAGE, opts);
		return 0;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run( CachedConfiguration.getInstance(), new SimpleTrendDetectionWholeRow(), args );
		System.exit(res);
	}

}
