/* To generate query text from TREC Microblog topics, parsed by TweetAnalyzer
 * Run: sh target/appassembler/bin/GenerateQueryText -queries {queryPath} -output {queryTextPath}
 */
package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;

import com.google.common.collect.Lists;

public class GenerateQueryText {
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String QUERIES_OPTION = "queries";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access" })
	public static void main(String[] args) throws Exception {

		Options options = new Options();

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

		if (!cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenerateQueryText.class.getName(), options);
			System.exit(-1);
		}

		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		BufferedWriter bw_text = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath)));
		
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		for ( TrecTopic topic : topics ) {  
			List<String> queryterms = parse(ANALYZER, topic.getQuery());
			for (String queryterm : queryterms) {
				bw_text.write(queryterm + " ");
			}
			bw_text.newLine();
		}
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
