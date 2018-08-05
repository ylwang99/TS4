/* Generate tweet text 2013 in the format of (docindex token1 token2 ...)
 * 
 * Run: sh target/appassembler/bin/GenerateTweetText_2013 -collection {collectionPath} -output {tweetTextPath} (-hourly)
 * 
 * Different from GenerateTweetText_2011.java in that this works for 2013 collection, 
 * where documents in the same hour may not be stored in the same file
 * and this is just to assume that each file stores all the docs from that hour 
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
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
import org.apache.lucene.util.Version;

import cc.twittertools.corpus.data.Status;
import cc.twittertools.index.TweetAnalyzer;
import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;

public class GenerateTweetText_2013 {
	private static final Logger LOG = Logger.getLogger(GenerateTweetText_2013.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String COLLECTION_OPTION = "collection";
	private static final String OUTPUT_OPTION = "output";
	private static final String HOURLY_OPTION = "hourly";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("collection location").create(COLLECTION_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output location").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasOptionalArg()
				.withDescription("generate stats hourly").create(HOURLY_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenerateTweetText_2013.class.getName(), options);
			System.exit(-1);
		}

		File collectionLocation = new File(cmdline.getOptionValue(COLLECTION_OPTION));
		JsonStatusCorpusReader stream = new JsonStatusCorpusReader(collectionLocation);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

		if (cmdline.hasOption(HOURLY_OPTION)) {
			generateTextHourly(stream, outputPath);
		} else {
			generateText(stream, outputPath);
		}
	}

	public static void generateText(JsonStatusCorpusReader stream, String outputPath) throws FileNotFoundException, IOException {
		BufferedWriter bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)));
		int cnt = 0;
		Status status;
		Set<String> set = new HashSet<String>();
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}
			bw_text.write(Integer.toString(cnt));
			List<String> terms = TweetParser.parseRemoveNone(ANALYZER, status.getText());
			for (String term : terms) {
				bw_text.write(" " + term);
				set.add(term);
			}

			cnt++;
			if (cnt % 100000 == 0) {
				LOG.info(cnt + " processed");
			}
			bw_text.newLine();
		}
		LOG.info("Total " + cnt + " processed");
		LOG.info("Vocab size: " + set.size());

		stream.close();
		bw_text.close();
	}

	public static void generateTextHourly(JsonStatusCorpusReader stream, String outputPath) throws FileNotFoundException, IOException {
		File outputFile = new File(outputPath);
		if (!outputFile.exists()) {
			outputFile.mkdir();
		}
		
		BufferedWriter bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/" + new File(outputPath).listFiles()[0].getName(), true)));
		int cnt = 0;
		Status status;
		String prev = "";
		Set<String> set = new HashSet<String>();
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}

			String curFileName = stream.getCurFileName();
			if (!curFileName.equals(prev)) {
				bw_text.close();
				String[] curFileNameSplit = curFileName.split("-");
				String month = curFileNameSplit[1];
				String day = curFileNameSplit[2];
				String hour = curFileNameSplit[3].substring(0, 2);
				bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/" + month + day + hour, true)));
				prev = curFileName;
			}

			bw_text.write(Integer.toString(cnt));
			List<String> terms = TweetParser.parseRemoveNone(ANALYZER, status.getText());
			for (String term : terms) {
				bw_text.write(" " + term);
				set.add(term);
			}

			cnt++;
			if (cnt % 100000 == 0) {
				LOG.info(cnt + " processed");
			}
			bw_text.newLine();
		}
		LOG.info("Total " + cnt + " processed");
		LOG.info("Vocab size: " + set.size());

		stream.close();
		bw_text.close();
	}
}
