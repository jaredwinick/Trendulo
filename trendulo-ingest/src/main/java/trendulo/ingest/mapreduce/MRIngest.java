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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import trendulo.ingest.MutationGenerator;
import trendulo.ingest.NGramGenerator;
import trendulo.ingest.StringUtilities;
import trendulo.ingest.TemporalNGram;

public class MRIngest extends Configured implements Tool {
	
	private static Options opts;
	private static Option passwordOpt;
	private static Option usernameOpt;
	private static String USAGE = "$ACCUMULO_HOME/bin/tool.sh trendulo-ingest.jar trendulo.ingest.mapreduce.MRIngest <instance name> <zoo keepers> <input directory> <output table>  [-u <username> -p password]";
	
	private static String nGramRowId = "!NGRAMS";
	private static String tweetsRowId = "!TWEETS";
	
	static {
		usernameOpt = new Option("u", "username", true, "username");
		passwordOpt = new Option("p", "password", true, "password");

		opts = new Options();

		opts.addOption(usernameOpt);
		opts.addOption(passwordOpt);
	}

	public static class Map extends Mapper<LongWritable,Text,Text,Mutation> {
		
		private Value emptyValue = new Value( new byte[0] );
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			// Each value is a row in the form of timestamp,status text
			// timestamp is always 13 characters
			String line = "";
			try {
				line = value.toString();
				String timestatmpString = line.substring(0, 13);
				long timestamp = Long.parseLong(timestatmpString);
				
				// Change the timestamp from GMT -> MDT (21600 seconds difference)
				timestamp -= 21600000;
				String message = line.substring(14);
			
			
			String cleanedStringSequence = StringUtilities.cleanStringSequence( message );
			// Generate the list of n-grams from the string sequence
			List<String> nGrams = NGramGenerator.generateAllNGramsInRange( cleanedStringSequence, 1, 3 );
			// For each nGram, build a TemporalNGram and generate a Mutation
			for ( String nGram : nGrams ) {
				 TemporalNGram temporalNGram = new TemporalNGram( nGram, timestamp );
				 Mutation mutation = MutationGenerator.generateMutationVLen( temporalNGram );
				 context.write(null, mutation);
			}	
			
			// Add a Mutation for total tweets and total ngrams
			Mutation totalNGramsMutation = MutationGenerator.generateMutationVLen( new TemporalNGram( nGramRowId, timestamp ), new Long( nGrams.size() ) );
			Mutation totalTweetsMutation = MutationGenerator.generateMutationVLen( new TemporalNGram( tweetsRowId, timestamp ) );
			context.write( null, totalNGramsMutation );
			context.write( null, totalTweetsMutation );
			
			}
			catch ( Exception e ) {
				System.out.println("Error parsing line:" + line);
			}
		}   
	}

	public int run(String[] unprocessed_args) throws Exception {

		Parser p = new BasicParser();
		CommandLine cl = p.parse(opts, unprocessed_args);
		String[] args = cl.getArgs();
		String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		if (args.length != 4) {
			System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 7.");
			return printUsage();
		}
		    
		String instanceName = args[0];
		String zookeepers = args[1];
		String inputDirectory = args[2];
		String outputTable = args[3];
		
		// Configure the MR job
		Job job = new Job( getConf(), MRIngest.class.getName() );
		job.setJarByClass( this.getClass() );
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( AccumuloOutputFormat.class );
		
		// Configure InputFormat
		TextInputFormat.addInputPaths(job, inputDirectory);
		
		// Configure OutputFormat
		job.setOutputFormatClass(AccumuloOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Mutation.class);
		AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username,
				password.getBytes(), true, outputTable);
		AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(),
				instanceName, zookeepers);
		   
		job.setMapperClass( Map.class );
	    job.setNumReduceTasks(0);
		job.waitForCompletion( true );
		
		return 0;
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
		int res = ToolRunner.run( CachedConfiguration.getInstance(), new MRIngest(), args );
		System.exit(res);
	}

}
