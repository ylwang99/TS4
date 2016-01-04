package ts4.ts4_core.tweets.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
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
		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		File input = new File(inputPath);
		File output = new File(outputPath);
		File[] files = input.listFiles();
		Arrays.sort(files);
		if (!output.exists()) {
			output.mkdir();
		}
		int cnt = 0;
		for (File inputFile : files) {
			FileStream stream = new FileStream();
			stream.arffFileOption = new FileOption("arffFile",'f',"ARFF file to load.",inputFile.getPath(),"arff",false);
			stream.restart();
			StreamKM streamKM = new StreamKM();
			streamKM.sizeCoresetOption.setValue(200 * clusterNums);
			streamKM.numClustersOption.setValue(clusterNums);
			streamKM.widthOption.setValue(20000);
			streamKM.setModelContext(stream.getHeader());
			streamKM.prepareForUse();
			while (stream.hasMoreInstances()){
			  DenseInstance trainInst = new DenseInstance(stream.nextInstance()); 
			  streamKM.trainOnInstanceImpl(trainInst);
			  cnt ++;
			  if (cnt % 10000 == 0) {
				  LOG.info(cnt + " processed.");
			  }
			}
			double[][] centers = new double[clusterNums][5];
			for (int i = 0; i < clusterNums; i ++) {
				centers[i] = streamKM.getClusteringResult().get(i).getCenter();
			}
			try {
				@SuppressWarnings("resource")
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath + "/" + inputFile.getName()));
				oos.writeObject(centers);
			} catch (FileNotFoundException e) {
				e.printStackTrace();  
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOG.info("total " + cnt + " processed.");
		
		
	}
}
