/* Generate input file to be fed into glove, basically, make the collection a one line document delimited by white space
 * Run: sh target/appassembler/bin/GenInput4Glove -input {inputPath} -output {onelineFile}
 */
package ts4.ts4_core.selectivesearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class GenInput4Glove {
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	
	@SuppressWarnings({ "static-access" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input location").create(INPUT_OPTION));
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
		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GenInput4Glove.class.getName(), options);
			System.exit(-1);
		}
		
		File input = new File(cmdline.getOptionValue(INPUT_OPTION));
		File output = new File(cmdline.getOptionValue(OUTPUT_OPTION));
		if (input.isDirectory()) {
			File[] files = input.listFiles();
			Arrays.sort(files);
			for (File file : files) {
				write(file, output);
			}
		} else {
			write(input, output);
		}
	}
	
	public static void write(File input, File output) throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));
		try {
			FileInputStream fis = new FileInputStream(input);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line;
			while((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				for (int i = 1; i < tokens.length; i ++) {
					bw.write(tokens[i] + " ");
				}
			}
			try {
				fis.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		bw.close();
	}
}