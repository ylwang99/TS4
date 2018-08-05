/* To store the vectors into a map so it's easier to generate vector representations for docs
 * 
 * Run: sh target/appassembler/bin/VectorToMap -vectors {vectorsPath} -output {vectorsMap}
 */
package ts4.ts4_core.glove;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
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

public class VectorToMap {
	private static final Logger LOG = Logger.getLogger(VectorToMap.class);
	
	private static final String VECTORS_OPTION = "vectors";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "resource" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
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
		
		if (!cmdline.hasOption(VECTORS_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(VectorToMap.class.getName(), options);
			System.exit(-1);
		}

		String vectorsPath = cmdline.getOptionValue(VECTORS_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		Map<String, float[]> map = new HashMap<String, float[]>();
		int cnt = 0;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(vectorsPath)));
			String line;
			while((line = br.readLine()) != null) {
				String[] arr = line.split(" ");
				float[] vector = new float[arr.length - 1];
				for (int i = 1; i < arr.length; i ++) {
					vector[i - 1] = Float.parseFloat(arr[i]);
				}
				map.put(arr[0], vector);
				
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
		
		try {
			ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(outputPath));
			output.writeObject(map);
		} catch (FileNotFoundException e) {
			e.printStackTrace();  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
