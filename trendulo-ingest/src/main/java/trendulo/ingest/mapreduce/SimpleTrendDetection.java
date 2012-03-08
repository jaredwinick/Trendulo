package trendulo.ingest.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SimpleTrendDetection extends Configured implements Tool {
	
	public static class Map extends Mapper<Key,Value,Text,DayCount> {
		@Override
		public void map(Key key, Value value, Context context)
				throws IOException, InterruptedException {

			Long count = Value.bytesToLong(value.get());
			int day = Integer.parseInt( key.getColumnQualifier().toString().substring(6, 8) );
			
			DayCount dc = new DayCount();
			dc.count = count;
			dc.day = day;
			
			//System.out.println("Key:" + key.getRow().toString() + " DAY:" + dc.toString());
			context.write(key.getRow(), dc);

		}   
	}
	
	public static class Reduce extends Reducer<Text,DayCount,Text,FloatWritable> {
		@Override
		public void reduce( Text key, Iterable<DayCount> values, Context context ) throws IOException, InterruptedException {
			
			List<DayCount> dayCounts = new ArrayList<DayCount>();
			for (DayCount dayCount : values) {
				dayCounts.add(new DayCount(dayCount));
			}
			
			if ( dayCounts.size() == 2 ) {
				System.out.println("KEY:" + key.toString() + " DCs:" + dayCounts.get(0));
				System.out.println("KEY:" + key.toString() + " DCs:" + dayCounts.get(1));
			
				DayCount day1 = dayCounts.get(0);
				DayCount day2 = dayCounts.get(1);
			

				float diff = 0;
				if (day1.day == 24 && day2.day == 25 && day2.count > 200 ) {
					diff = ((float) day2.count / (float) day1.count);
				} else if (day1.day == 25 && day2.day == 24 && day1.count > 200 ) {
					diff = ((float) day1.count / (float) day2.count);
				}
				if ( diff != 0  )
				    context.write(key, new FloatWritable(diff));		
			}
			
		}
	}
	
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run( CachedConfiguration.getInstance(), new SimpleTrendDetection(), args );
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = new Job( getConf(), SimpleTrendDetection.class.getName() );
		job.setJarByClass( this.getClass() );
		job.setInputFormatClass( AccumuloInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
		
		// Configure InputFormat
		AccumuloInputFormat.setInputInfo( job.getConfiguration(), "root", "secret".getBytes(), "tweets", new Authorizations() );
		AccumuloInputFormat.setZooKeeperInstance( job.getConfiguration(), "trendulo", "192.168.1.101" );
		ArrayList<Pair<Text,Text>> columns = new ArrayList<Pair<Text,Text>>();
		columns.add( new Pair<Text,Text>(new Text("DAY"),new Text("20120124")));
		columns.add( new Pair<Text,Text>(new Text("DAY"),new Text("20120125")));
		AccumuloInputFormat.fetchColumns( job.getConfiguration(), columns );
		
		ArrayList<Range> ranges = new ArrayList<Range>();
		ranges.add( new Range( new Text("b"), new Text("bb") ) );
		//AccumuloInputFormat.setRanges( job.getConfiguration(), ranges);
		
		// Configure OutputFormat
		TextOutputFormat.setOutputPath(job, new Path("/user/jared/trending") );
		job.setOutputKeyClass( Text.class );
		job.setMapOutputValueClass( DayCount.class );
		job.setOutputValueClass( FloatWritable.class );
		
		job.setMapperClass( Map.class );
		job.setReducerClass( Reduce.class );
		
		job.waitForCompletion( true );
		
		return 0;
	}

}
