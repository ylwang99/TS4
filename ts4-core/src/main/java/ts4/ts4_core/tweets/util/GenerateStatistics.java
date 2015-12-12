/* Generate statistics used for running queries
 * Run: sh target/appassembler/bin/GenerateStatistics -index {indexPath} -collection {collectionPath} -output {statisticsPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.List;

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

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.HMapKI;

public class GenerateStatistics {
	public static int NUM_DOCS = 16141812;
	public static int TOTAL_TERMS = 203429861;
	public static int TOP = 1000;
	
	private static final Logger LOG = Logger.getLogger(GenerateStatistics.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";
	private static final String COLLECTION_OPTION = "collection";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("collection location").create(COLLECTION_OPTION));
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

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenerateStatistics.class.getName(), options);
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
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		if (!new File(outputPath).exists()) {
			new File(outputPath).mkdir();
		}
		new File(outputPath + "/cf").mkdir();
		
		LOG.info("Reading term statistics.");
		TermStatistics termStats = new TermStatistics(indexPath, NUM_DOCS);
		LOG.info("Finished reading term statistics.");

		StatusStream stream = new JsonStatusCorpusReader(collectionLocation);
		BufferedWriter bw_id = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_id.txt")));
		BufferedWriter bw_length = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length.txt")));
		BufferedWriter bw_length_encoded = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length_encoded.txt")));
		BufferedWriter bw_term_ordered = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/all_terms_ordered.txt")));
		BufferedWriter bw_tf = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/all_terms_tf.txt")));
		BufferedWriter bw_length_ordered = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/doc_length_ordered.txt"))); 
		BufferedWriter bw_cf = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/cf_table.txt")));

		for (int id = 1; id <= termStats.getId2Term().size(); id ++) {
			bw_cf.write(String.valueOf(termStats.getFreq(id)));
			bw_cf.newLine();
		}
		bw_cf.close();

		LOG.info("Finished writing cf_table.");

		ObjectOutputStream oos_cf_hour = null;
		ObjectOutputStream oos_cf_day = null;
		CFStats cfstats_hour = new CFStats();
		CFStats cfstats_day = new CFStats();
		HMapKI<String> cf2Freq_hour = new HMapKI<String>(); 
		HMapKI<String> cf2Freq_day = new HMapKI<String>();
		int totalTerm_hour = 0;
		int totalTerm_day = 0;
		int hour_idx = 1;
		int day_idx = 1;
		String prev = "";
		int cnt = 0;
		Status status;
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}
			bw_id.write(String.valueOf(status.getId()));
			bw_id.newLine();

			List<String> terms = parse(ANALYZER, status.getText());
			bw_length.write(String.valueOf(terms.size()));
			bw_length.newLine();
			bw_length_encoded.write(String.valueOf(decodeNormValue(encodeNormValue(1.0f, terms.size()))));
			bw_length_encoded.newLine();

			HMapII docVector = new HMapII();
			HMapII docTermFreq = new HMapII();
			for (String term : terms) {
				int termId = termStats.getId(term);
				if (docVector.containsKey(termId)) {
					docTermFreq.put(termId, docTermFreq.get(termId) + 1);
				} else {
					docTermFreq.put(termId, 1);
					docVector.put(termId, termStats.getDf(termId));
				}
				cf2Freq_hour.increment(term);
				cf2Freq_day.increment(term);
				totalTerm_hour ++;
				totalTerm_day ++;
			}
			bw_length_ordered.write(String.valueOf(docVector.size()));
			bw_length_ordered.newLine();

			for (edu.umd.cloud9.util.map.MapII.Entry entry : docVector.entrySet()) {
				int key = entry.getKey();
				bw_term_ordered.write(String.valueOf(key));
				bw_term_ordered.newLine();
				bw_tf.write(String.valueOf(docTermFreq.get(key)));
				bw_tf.newLine();
			}	
			
			String createdAt = status.getCreatedAt();
			String createdHour = createdAt.split(":")[0];
			if (!createdHour.equals(prev)) {
				if (!prev.equals("")) {
					cfstats_hour.setCf(cf2Freq_hour);
					cfstats_hour.setTotalTermCnt(totalTerm_hour);
					oos_cf_hour = new ObjectOutputStream(new FileOutputStream(outputPath + "/cf/hour" + hour_idx));
					oos_cf_hour.writeObject(cfstats_hour);
					hour_idx ++;
					cf2Freq_hour = new HMapKI<String>();
					totalTerm_hour = 0;
					String[] createdHourArr = createdHour.split(" ");
					String hour = createdHourArr[3];
					if (hour.equals("00")) {
						cfstats_day.setCf(cf2Freq_day);
						cfstats_day.setTotalTermCnt(totalTerm_day);
						oos_cf_day = new ObjectOutputStream(new FileOutputStream(outputPath + "/cf/day" + day_idx));
						oos_cf_day.writeObject(cfstats_day);
						day_idx ++;
						cf2Freq_day = new HMapKI<String>();
						totalTerm_day = 0;
					}
				}
				prev = createdHour;
			}
			
			cnt++;
			if (cnt % 100000 == 0) {
				LOG.info(cnt + " processed");
			}
		}
		cfstats_hour.setCf(cf2Freq_hour);
		cfstats_hour.setTotalTermCnt(totalTerm_hour);
		oos_cf_hour = new ObjectOutputStream(new FileOutputStream(outputPath + "/cf/hour" + hour_idx));
		oos_cf_hour.writeObject(cfstats_hour);
		cfstats_day.setCf(cf2Freq_day);
		cfstats_day.setTotalTermCnt(totalTerm_day);
		oos_cf_day = new ObjectOutputStream(new FileOutputStream(outputPath + "/cf/day" + day_idx));
		oos_cf_day.writeObject(cfstats_day);
		LOG.info("Total " + cnt + " processed");

		stream.close();
		bw_id.close();
		bw_length.close();
		bw_length_encoded.close();
		bw_term_ordered.close();
		bw_tf.close();
		bw_length_ordered.close();
		oos_cf_hour.close();
		oos_cf_day.close();
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

