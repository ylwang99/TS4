/* Generate statistics used for running queries
 * Run: sh target/appassembler/bin/GenerateStatistics_CFPerQuery -index {indexPath} -collection {collectionPath} -queries {queryPath} -output {statisticsPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.Version;

import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.HMapKI;

public class GenerateStatistics_CFPerQuery {
	public static int NUM_DOCS = 16141812;
	public static int TOTAL_TERMS = 203429861;
	public static int TOP = 1000;
	
	private static final Logger LOG = Logger.getLogger(GenerateStatistics_CFPerQuery.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";
	private static final String COLLECTION_OPTION = "collection";
	private static final String QUERIES_OPTION = "queries";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("collection location").create(COLLECTION_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output location").create(OUTPUT_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenerateStatistics_CFPerQuery.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
		File collectionLocation = new File(collectionPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + collectionLocation + " does not exist!");
			System.exit(-1);
		}
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		if (!new File(outputPath).exists()) {
			new File(outputPath).mkdir();
		}
		new File(outputPath + "/cf/query").mkdir();
		
		LOG.info("Reading term statistics.");
		TermStatistics termStats = new TermStatistics(indexPath, NUM_DOCS);
		LOG.info("Finished reading term statistics.");

		StatusStream stream = new JsonStatusCorpusReader(collectionLocation);
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		List<Long> querytime = new ArrayList<Long>();
		Set<Long> querytimeSet = new HashSet<Long>();
		for (TrecTopic topic : topics) {
			querytime.add(topic.getQueryTweetTime());
			querytimeSet.add(topic.getQueryTweetTime());
		}
		
		ObjectOutputStream oos_cf = null;
		CFStats cfstats = new CFStats();
		HMapKI<String> cf2Freq = new HMapKI<String>(); 
		int totalTerm = 0;
		int cnt = 0;
		Status status;
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}

			List<String> terms = parse(ANALYZER, status.getText());
			for (String term : terms) {
				cf2Freq.increment(term);
				totalTerm ++;
			}
			if (querytimeSet.contains(status.getId())) {
				for (int idx = 0; idx < querytime.size(); idx ++) {
					if (querytime.get(idx) == status.getId()) {
						cfstats.setCf(cf2Freq);
						cfstats.setTotalTermCnt(totalTerm);
						oos_cf = new ObjectOutputStream(new FileOutputStream(outputPath + "/cf/query/query" + (idx + 1)));
						oos_cf.writeObject(cfstats);
					}
				}
			}
			cnt++;
			if (cnt % 100000 == 0) {
				LOG.info(cnt + " processed");
			}
		}
		LOG.info("Total " + cnt + " processed");

		stream.close();
		oos_cf.close();
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

	private static final float[] NORM_TABLE = new float[256];

	static {
		for (int i = 0; i < 256; i++) {
			float floatNorm = SmallFloat.byte315ToFloat((byte)i);
			NORM_TABLE[i] = 1.0f / (floatNorm * floatNorm);
		}
	}

	public static float decodeNormValue(byte norm) {
		return NORM_TABLE[norm & 0xFF];  // & 0xFF maps negative bytes to positive above 127
	}

	public static byte encodeNormValue(float boost, float length) {
		return SmallFloat.floatToByte315((boost / (float) Math.sqrt(length)));
	}
}

