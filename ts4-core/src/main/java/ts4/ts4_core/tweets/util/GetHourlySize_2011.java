/* Get tweet hourly size in 2011
 * 
 * Run: sh target/appassembler/bin/GetHourlySize_2011 -collection {collectionPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.index.TweetAnalyzer;
import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;

public class GetHourlySize_2011 {
	private static final Logger LOG = Logger.getLogger(GetHourlySize_2011.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String COLLECTION_OPTION = "collection";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("collection location").create(COLLECTION_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(COLLECTION_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GetHourlySize_2011.class.getName(), options);
			System.exit(-1);
		}

		File collectionLocation = new File(cmdline.getOptionValue(COLLECTION_OPTION));
		StatusStream stream = new JsonStatusCorpusReader(collectionLocation);
		
		String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
		Map<String, String> monthMap = new HashMap<String, String>();
		for (int i = 0; i < months.length; i ++) {
			monthMap.put(months[i], String.format("%02d", i + 1));
		}
		int hourlySize = 0;
		int cnt = 0;
		Status status;
		String prev = "";
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}
			String createdAt = status.getCreatedAt();
			String createdHour = createdAt.split(":")[0];
			if (!createdHour.equals(prev)) {
				if (!prev.equals("")) {
					System.out.println(hourlySize);
				}
				prev = createdHour;
				hourlySize = 0;
			}

			hourlySize ++;
			cnt++;
		}
		LOG.info("Total " + cnt + " processed");

		stream.close();
	}
}
