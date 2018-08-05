/* Generate vector representations for docs based on word vectors
 * 
 * Run: sh target/appassembler/bin/DocToVec -input {textPath} -vectors {vectorsMap} -output {vectorPath}
 * 
 * input is in the format of (docindex token1 token2 ...)
 */
package ts4.ts4_core.glove;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class DocToVec {
	private static final Logger LOG = Logger.getLogger(DocToVec.class);
	
	private static final String INPUT_OPTION = "input";
	private static final String VECTORS_OPTION = "vectors";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "unchecked" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input location").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("vectors location").create(VECTORS_OPTION));
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
		
		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(VECTORS_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(DocToVec.class.getName(), options);
			System.exit(-1);
		}
		
		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String vectorsPath = cmdline.getOptionValue(VECTORS_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		Map<String, float[]> map = new HashMap<String, float[]>();
		try {
			@SuppressWarnings("resource")
			ObjectInputStream vectorsInput = new ObjectInputStream(new FileInputStream(vectorsPath));
			map = (HashMap<String, float[]>)vectorsInput.readObject();
		} catch (FileNotFoundException e) {
			e.printStackTrace();  
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOG.info("Done reading word vectors.");

		File input = new File(inputPath);
		File output = new File(outputPath);
		int dimension = map.entrySet().iterator().next().getValue().length;
		if (input.isDirectory()) {
			File[] files = input.listFiles();
			Arrays.sort(files);
			if (!output.exists()) {
				output.mkdir();
			}
			for (File file : files) {
				write(file, new File(outputPath + "/" + file.getName()), dimension, map);
			}
		} else {
			write(input, output, dimension, map);
		}
	}

	public static void write(File input, File output, int dimension, Map<String, float[]> map) throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)));
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input)));
			String line;
			while((line = br.readLine()) != null) {
				float[] sum = new float[dimension];
				String[] tokens = line.split(" ");
				bw.write(tokens[0]);
				for (int i = 1; i < tokens.length; i ++) {
					if (map.containsKey(tokens[i])) {
						for (int j = 0; j < dimension; j ++) {
							sum[j] += map.get(tokens[i])[j];
						}
					}
				}
				if (tokens.length > 1) {
					for (int i = 0; i < dimension; i ++) {
						sum[i] /= tokens.length - 1;
					}
				}
				for (int i = 0; i < dimension; i ++) {
					bw.write(" " + sum[i]);
				}
				bw.newLine();
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
		bw.close();
	}
}
