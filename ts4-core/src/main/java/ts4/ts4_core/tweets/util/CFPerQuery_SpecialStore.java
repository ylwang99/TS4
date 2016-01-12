/* Print only query term cfs 
 * Run: sh target/appassembler/bin/CFPerQuery_SpecialStore
 * 		-index {indexPath}
 * 		-stats {queryCFPath}
 * 		-queries {queriesPath}
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
import java.util.List;
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
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import ts4.ts4_core.tweets.util.*;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.util.TopNScoredInts;

public class CFPerQuery_SpecialStore {
	private static final Logger LOG = Logger.getLogger(CFPerQuery_SpecialStore.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);
	private static final int DAYS = 17;

	private static final String INDEX_OPTION = "index";
	private static final String STATS_OPTION = "stats";
	private static final String QUERIES_OPTION = "queries";

	@SuppressWarnings({ "static-access" })
	public static void main(String[] args) throws Exception {
		float mu = 2500.0f;
		int numResults = 1000;

		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("statistics location").create(STATS_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(STATS_OPTION) || !cmdline.hasOption(QUERIES_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(CFPerQuery_SpecialStore.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
		TermStatistics termStats = new TermStatistics(indexPath, GenerateStatistics.NUM_DOCS);
		String statsPath = cmdline.getOptionValue(STATS_OPTION);
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for ( @SuppressWarnings("unused") TrecTopic topic : topics ) {
			topicTotal ++;
		}
		CFStats[] cf = new CFStats[topicTotal];
		ObjectInputStream ois = null;
		try {
			File[] files = new File(statsPath).listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("query")) {
					ois = new ObjectInputStream(new FileInputStream(file.getPath()));
					cf[Integer.parseInt(file.getName().substring(5)) - 1] = (CFStats) ois.readObject();
				}
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
		ois.close();
		
		int topicCnt = 0;
		for ( TrecTopic topic : topics ) {  
			List<String> queryterms = parse(ANALYZER, topic.getQuery());
			for (String term : queryterms) {
				System.out.print(cf[topicCnt].getFreq(term) + " ");
				System.out.print(cf[topicCnt].getTotalTermCnt());
			}
			System.out.println();
		}
	}

	public static List<String> parse(Analyzer analyzer, String s) throws IOException {
		List<String> list = Lists.newArrayList();

		TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));
		CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			list.add(cattr.toString());
		}
		tokenStream.end();
		tokenStream.close();

		return list;
	}
}
