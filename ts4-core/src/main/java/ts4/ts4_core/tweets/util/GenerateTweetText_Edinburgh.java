/* Generate Edinburgh tweet text in the format of (docindex token1 token2 ...)
 * 
 * Run: sh target/appassembler/bin/GenerateTweetText_Edinburgh -collection {collectionPath} -output {tweetTextPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;

public class GenerateTweetText_Edinburgh {
	private static final Logger LOG = Logger.getLogger(GenerateTweetText_Edinburgh.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String COLLECTION_OPTION = "collection";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

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

		if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenerateTweetText_Edinburgh.class.getName(), options);
			System.exit(-1);
		}

		File collectionLocation = new File(cmdline.getOptionValue(COLLECTION_OPTION));
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath, true)));
		int cnt = 0;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(collectionLocation)));
			String line;
			while((line = br.readLine()) != null) {
				String[] arr = line.split("\t");
				bw.write(cnt);
				List<String> tokens = TweetParser.parseRemoveNone(ANALYZER, arr[2]);
				for (String token : tokens) {
					bw.write(" " + token);
				}
				bw.newLine();
				
				cnt ++;
				if (cnt % 100000 == 0) {
					LOG.info(cnt + " processed");
				}
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		LOG.info("Total " + cnt + " processed");
		
		bw.close();
	}
}
