package ts4.ts4_core.tweets.util;

/* Generate input sequence file for mahout streaming clustering
 * Run: sh target/appassembler/bin/TexttoSequence -input {tweetVecPath} -output {tweetSequencePath}
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

public class VecToSequence {
	private static final Logger LOG = Logger.getLogger(VecToSequence.class);
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "deprecation" })
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
			formatter.printHelp(VecToSequence.class.getName(), options);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		File input = new File(inputPath);
		File output = new File(outputPath);
		File[] files = input.listFiles();
		Arrays.sort(files);
		if (!output.exists()) {
			output.mkdir();
		}
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer = null;
		int cnt = 0;
		for (File file : files) {
			FileInputStream fis = new FileInputStream(file.getPath());
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			IntWritable key = new IntWritable();
			VectorWritable value = new VectorWritable();
			SequenceFile.createWriter(fs, conf, new Path(outputPath + "/" + file.getName()), key.getClass(), value.getClass());
			String line;
			while((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				key.set(Integer.parseInt(tokens[0]));
				Vector vector = new DenseVector(tokens.length - 1);
				for (int i = 1; i < tokens.length; i ++) {
					vector.set(i - 1, Double.parseDouble(tokens[i]));
				}
				value.set(vector);
				if (cnt % 100000 == 0) {
					LOG.info(cnt + " processed.");
				}
				cnt ++;
			}
			try {
				fis.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOG.info("total " + cnt + " processed.");
		IOUtils.closeStream(writer); 
	}
}
