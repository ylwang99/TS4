package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;

/* MOA streaming clustering
 * Run: sh target/appassembler/bin/MoaStreaming -input {tweetVecPath} -output {ArffPath}
 * 
 */
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import ts4.ts4_core.moa.clusterers.streamkm.*;
import moa.options.FileOption;
import moa.streams.clustering.FileStream;
import weka.core.DenseInstance;

public class test {
	private static final Logger LOG = Logger.getLogger(test.class);
	private static final String INPUT_OPTION = "input";

	@SuppressWarnings({ "static-access"})
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input location").create(INPUT_OPTION));
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
		int cnt = 0;
		try {
			@SuppressWarnings("resource")
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputPath));
			double[][] centers = (double[][])ois.readObject();
			for (int i = 0; i < centers.length; i ++) {
				for (int j = 0; j < centers[i].length; j ++) {
					System.out.print(centers[i][j] + " ");
				}
				System.out.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
