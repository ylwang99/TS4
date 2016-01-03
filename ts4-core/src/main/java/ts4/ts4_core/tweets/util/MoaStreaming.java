package ts4.ts4_core.tweets.util;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

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

import moa.clusterers.streamkm.StreamKM;
import moa.options.FileOption;
import moa.streams.clustering.FileStream;
import weka.core.DenseInstance;

public class MoaStreaming {
	private static final Logger LOG = Logger.getLogger(MoaStreaming.class);
	private static final String INPUT_OPTION = "input";
	private static final String OUTPUT_OPTION = "output";
	private static final int clusterNums = 100;

	@SuppressWarnings({ "static-access"})
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
			formatter.printHelp(MoaStreaming.class.getName(), options);
			System.exit(-1);
		}

		FileStream stream = new FileStream();
		String inputFile = cmdline.getOptionValue(INPUT_OPTION);	//dense ARFF file
		stream.arffFileOption = new FileOption("arffFile",'f',"ARFF file to load.",inputFile,"arff",false);
		stream.restart();
		StreamKM streamKM = new StreamKM();
		streamKM.sizeCoresetOption.setValue(200 * clusterNums);
		streamKM.numClustersOption.setValue(clusterNums);
		streamKM.widthOption.setValue(30000);
		streamKM.setModelContext(stream.getHeader());
		streamKM.prepareForUse();
		int cnt = 0;
		while (stream.hasMoreInstances()){
		  DenseInstance trainInst = new DenseInstance(stream.nextInstance()); 
		  streamKM.trainOnInstanceImpl(trainInst);
		  cnt ++;
		  if (cnt % 10000 == 0) {
			  LOG.info(cnt + "processed.");
		  }
		}
		LOG.info("total " + cnt + " processed.");
		
		String outputFile = cmdline.getOptionValue(OUTPUT_OPTION);
		try {
			@SuppressWarnings("resource")
			ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(outputFile));
			output.writeObject(streamKM);
		} catch (FileNotFoundException e) {
			e.printStackTrace();  
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
