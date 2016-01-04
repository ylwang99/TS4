/* Run queries on streaming kmeans results daily with cf up till the query time
 * Run: sh target/appassembler/bin/RunQueriesDaily_Streaming_CFPerQuery -index {indexPath} 
 * 		-stats {statsPath} -docsvector {docVectorPath} -dayclusters {dayclustersPath} -hourclusters {hourclustersPath}
 * 		-dayhours {dayFile} [-hourly true] -dimension {dimension} -partition {partitionNum} -top {N}
 * 		-queries {queriesPath} -queriesvector {queryVectorPath} -output {outputPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;
import org.apache.mahout.math.VectorWritable;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import ts4.ts4_core.tweets.util.*;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.util.TopNScoredInts;

public class test {
	private static final Logger LOG = Logger.getLogger(test.class);

	private static final String INPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "deprecation" })
	public static void main(String[] args) throws Exception {
		float mu = 2500.0f;
		int numResults = 1000;

		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("output location").create(INPUT_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(test.class.getName(), options);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		
		Configuration conf = new Configuration();
		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();
		SequenceFile.Reader reader = null;
		int cnt = 0;
		try {
			Option option = SequenceFile.Reader.file(new Path(inputPath + "/part-r-00000"));
			reader = new SequenceFile.Reader(conf, option);
			while (reader.next(key, val)) {
				for (int i = 0; i < val.get().size(); i ++) {
					System.out.print(val.get().get(i) + " ");
				}
				System.out.println();
		    }
            reader.close();
		} catch(Exception e){
            System.out.println("File not found");
		}
		LOG.info("Finished reading cluster centers and assignments from file.");
		
	}

}
