/* Generate input file for moa streaming clustering
 * 
 * Run: sh target/appassembler/bin/VecToArffFile -input {tweetVecPath} -dimension {dimension} -output {arffPath}
 */
package ts4.ts4_core.moa.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class VecToArffFile {
	private static final Logger LOG = Logger.getLogger(VecToArffFile.class);
	
	private static final String INPUT_OPTION = "input";
	private static final String DIMENSION_OPTION = "dimension";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input location").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION_OPTION));
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
		
		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(DIMENSION_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(VecToArffFile.class.getName(), options);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION_OPTION));
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

		File input = new File(inputPath);
		File output = new File(outputPath);
		File[] files = input.listFiles();
		Arrays.sort(files);
		if (!output.exists()) {
			output.mkdir();
		}
		
		int cnt = 0;
		for (File file : files) {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/" + file.getName())));
			bw.write("@RELATION hour");
			bw.newLine();
			for (int i = 0; i < dimension; i ++) {
				bw.write("@ATTRIBUTE attribute" + (i + 1) +"  NUMERIC");
				bw.newLine();
			}
			bw.write("@DATA");
			bw.newLine();
			String line;
			while((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				for (int i = 1; i < tokens.length - 1; i ++) {
					bw.write(tokens[i] + ",");
				}
				bw.write(tokens[dimension]);
				bw.newLine();
				
				cnt ++;
				if (cnt % 1000000 == 0) {
					LOG.info(cnt + " processed");
				}
			}
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			bw.close();
		}
		LOG.info("Total " + cnt + " processed");
	}
}
