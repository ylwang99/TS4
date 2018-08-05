/* Generate statistics used for running queries
 * Run: sh target/appassembler/bin/GenerateStatistics -index {indexPath} -collection {collectionPath} -queries {queryPath} -output {statisticsPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.SmallFloat;
import org.apache.lucene.util.Version;

import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.HMapKI;
import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;

public class GenerateStatistics {
//	public static int NUM_DOCS = 259057030; // 16141812;
//	public static long TOTAL_TERMS = 3113705431L; // 203429861;
	
	private static final Logger LOG = Logger.getLogger(GenerateStatistics.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

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
			formatter.printHelp(GenerateStatistics.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		File collectionLocation = new File(cmdline.getOptionValue(COLLECTION_OPTION));
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
		
		if (!new File(outputPath).exists()) {
			new File(outputPath).mkdir();
		}
		new File(outputPath + "/cf/").mkdir();

		LOG.info("Reading term statistics");
		TermStatistics termStats = new TermStatistics(indexPath);
		LOG.info("Finished reading term statistics");

		StatusStream stream = new JsonStatusCorpusReader(collectionLocation);
		BufferedWriter bw_id = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_id.txt")));
		BufferedWriter bw_length = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length.txt")));
		BufferedWriter bw_length_encoded = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length_encoded.txt")));
		BufferedWriter bw_term_ordered = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/all_terms_ordered.txt")));
		BufferedWriter bw_tf = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/all_terms_tf.txt")));
		BufferedWriter bw_length_ordered = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length_ordered.txt"))); 
		BufferedWriter bw_cf = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/cf_table.txt")));
		BufferedWriter bw_cf_perquery = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/cf/" + "cf-" + queryPath.substring(queryPath.lastIndexOf("/") + 1))));
		BufferedWriter bw_stats = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/stats.txt")));
		
		for (int id = 1; id <= termStats.getVocabSize(); id ++) {
			bw_cf.write(String.valueOf(termStats.getFreq(id)));
			bw_cf.newLine();
		}
		bw_cf.close();
		LOG.info("Finished writing cf_table");

		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		Map<Long, Integer> queryTimeMap = new HashMap<Long, Integer>();
		Map<Integer, List<String>> queryTermsMap = new HashMap<Integer, List<String>>(); 
		for (TrecTopic topic : topics) {
			queryTimeMap.put(topic.getQueryTweetTime(), topicTotal);
			queryTermsMap.put(topicTotal, TweetParser.parse(ANALYZER, topic.getQuery()));
			topicTotal ++;
		}

		HMapKI<String> cf2Freq = new HMapKI<String>(); 
		String[] cfPerQuery = new String[topicTotal];
		int cnt = 0;
		long totalTerm = 0;
		long uniqueTerm = 0;
		Status status;
		while ((status = stream.next()) != null) {
			long id = status.getId();
			if (status.getText() != null) {
				bw_id.write(String.valueOf(id));
				bw_id.newLine();

				List<String> terms = TweetParser.parse(ANALYZER, status.getText());
				bw_length.write(String.valueOf(terms.size()));
				bw_length.newLine();
				bw_length_encoded.write(String.valueOf(decodeNormValue(encodeNormValue(1.0f, terms.size()))));
				bw_length_encoded.newLine();

				HMapII docTermFreq = new HMapII();
				for (String term : terms) {
					int termId = termStats.getId(term);
					if (docTermFreq.containsKey(termId)) {
						docTermFreq.put(termId, docTermFreq.get(termId) + 1);
					} else {
						docTermFreq.put(termId, 1);
					}
					cf2Freq.increment(term);
					totalTerm ++;
				}
				bw_length_ordered.write(String.valueOf(docTermFreq.size()));
				bw_length_ordered.newLine();
				uniqueTerm += docTermFreq.size();

				for (edu.umd.cloud9.util.map.MapII.Entry entry : docTermFreq.entrySet()) {
					int key = entry.getKey();
					bw_term_ordered.write(String.valueOf(key));
					bw_term_ordered.newLine();
					bw_tf.write(String.valueOf(docTermFreq.get(key)));
					bw_tf.newLine();
				}
				
				cnt++;
				if (cnt % 100000 == 0) {
					LOG.info(cnt + " processed");
				}
			}
			if (queryTimeMap.containsKey(id)) {
				int idx = queryTimeMap.get(id);
				cfPerQuery[idx] = "";
				for (String term : queryTermsMap.get(idx)) {
					cfPerQuery[idx] += String.valueOf(cf2Freq.get(term)) + " ";
				}
				cfPerQuery[idx] += String.valueOf(totalTerm);
			}
		}
		LOG.info("Total " + cnt + " processed");
		for (int i = 0; i < topicTotal; i ++) {  
			bw_cf_perquery.write(cfPerQuery[i]);
			bw_cf_perquery.newLine();
		}
		
		bw_stats.write(String.valueOf(cnt));
		bw_stats.newLine();
		bw_stats.write(String.valueOf(totalTerm));
		bw_stats.newLine();
		bw_stats.write(String.valueOf(uniqueTerm));
		bw_stats.newLine();
		bw_stats.write(String.valueOf(termStats.getVocabSize()));
		bw_stats.newLine();

		stream.close();
		bw_id.close();
		bw_length.close();
		bw_length_encoded.close();
		bw_term_ordered.close();
		bw_tf.close();
		bw_length_ordered.close();
		bw_cf_perquery.close();
		bw_stats.close();
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

