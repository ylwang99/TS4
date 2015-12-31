package ts4.ts4_core.tweets.util;

/* Check sequence file
 * Run: sh target/appassembler/bin/CheckFile -input {tweetVecPath}
 * 
 */
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.mahout.clustering.streaming.mapreduce.CentroidWritable;

public class CheckFile {
	private static final Logger LOG = Logger.getLogger(CheckFile.class);
	private static final String INPUT_OPTION = "input";

	@SuppressWarnings({ "static-access", "deprecation" })
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
			formatter.printHelp(CheckFile.class.getName(), options);
			System.exit(-1);
		}

		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		IntWritable key = new IntWritable();
		CentroidWritable val = new CentroidWritable();
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(inputPath), conf);		
		while (reader.next(key, val)) {
	        System.err.println(key + "\t" + val);
	    }
	    reader.close();
	}
}
