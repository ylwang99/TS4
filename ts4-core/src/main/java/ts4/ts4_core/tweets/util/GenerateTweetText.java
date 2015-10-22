/* Generate tweet text in the format of (docindex token1 token2 ...)
 * Run: sh target/appassembler/bin/GenerateTweetText -collection {collectionPath} -output {tweetTextPath} (-hourly)
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

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

import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;
import cc.twittertools.corpus.data.Status;
import cc.twittertools.corpus.data.StatusStream;
import cc.twittertools.index.TweetAnalyzer;

import com.google.common.collect.Lists;

public class GenerateTweetText {
	private static final Logger LOG = Logger.getLogger(GenerateTweetText.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

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
			formatter.printHelp(GenerateTweetText.class.getName(), options);
			System.exit(-1);
		}

		File collectionLocation = new File(cmdline.getOptionValue(COLLECTION_OPTION));
		StatusStream stream = new JsonStatusCorpusReader(collectionLocation);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		if (cmdline.hasOption(HOURLY_OPTION)) {
			generateTextHourly(stream, outputPath);
		} else {
			generateText(stream, outputPath);
		}
	}
	
	public static void generateText(StatusStream stream, String outputPath) throws FileNotFoundException, IOException {
		BufferedWriter bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)));
		int cnt = 0;
		Status status;
		Set<String> set = new HashSet<String>();
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}
			bw_text.write(cnt);
			List<String> terms = parse(ANALYZER, status.getText());
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
	
	public static void generateTextHourly(StatusStream stream, String outputPath) throws FileNotFoundException, IOException {
		if (!new File(outputPath).exists()) {
			new File(outputPath).mkdir();
		}
		String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
		Map<String, String> monthMap = new HashMap<String, String>();
		for (int i = 0; i < months.length; i ++) {
			monthMap.put(months[i], String.format("%02d", i + 1));
		}
		BufferedWriter bw_text = null;
		int cnt = 0;
		Status status;
		String prev = "";
		Set<String> set = new HashSet<String>();
		while ((status = stream.next()) != null) {
			if (status.getText() == null) {
				continue;
			}
			String createdAt = status.getCreatedAt();
			String createdHour = createdAt.split(":")[0];
			if (!createdHour.equals(prev)) {
				if (!prev.equals("")) {
					bw_text.close();
				}
				String[] createdHourArr = createdHour.split(" ");
				String month = monthMap.get(createdHourArr[1]);
				String day = createdHourArr[2];
				String hour = createdHourArr[3];
				bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/" + month + day + hour)));
				prev = createdHour;
			}
			
			bw_text.write(cnt);
			List<String> terms = parse(ANALYZER, status.getText());
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
