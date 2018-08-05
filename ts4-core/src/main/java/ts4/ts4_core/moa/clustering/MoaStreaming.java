/* Moa Streaming Clustering
 * 
 * Run: sh target/appassembler/bin/MoaStreaming -input {arffPath} -dimension {dimension} -partition {partitionNum} -output {outputPath}
 */
package ts4.ts4_core.moa.clustering;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import moa.options.FileOption;
import moa.streams.clustering.FileStream;
import ts4.ts4_core.moa.clusterers.streamkm.StreamKM;
import weka.core.DenseInstance;

public class MoaStreaming {
	private static final Logger LOG = Logger.getLogger(MoaStreaming.class);
	
	private static final String INPUT_OPTION = "input";
	private static final String DIMENSION_OPTION = "dimension";
	private static final String PARTITION = "partition";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "resource"})
	public static void main(String[] args) throws Exception {
		Options options = new Options();
		
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input location").create(INPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition number").create(PARTITION));
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
		
		if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(DIMENSION_OPTION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(MoaStreaming.class.getName(), options);
			System.exit(-1);
		}
		
		String inputPath = cmdline.getOptionValue(INPUT_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION_OPTION));
		int clusterNums = Integer.parseInt(cmdline.getOptionValue(PARTITION));
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
			LOG.info(inputFile.getPath());
			
			FileStream stream = new FileStream();
			stream.arffFileOption = new FileOption("arffFile", 'f', "ARFF file to load.", inputFile.getPath(), "arff", false);
			stream.restart();
			StreamKM streamKM = new StreamKM();
			streamKM.sizeCoresetOption.setValue(50 * clusterNums);
			streamKM.numClustersOption.setValue(clusterNums);
			streamKM.widthOption.setValue(100000);
			streamKM.setModelContext(stream.getHeader());
			streamKM.prepareForUse();
			
			while (stream.hasMoreInstances()){
				DenseInstance trainInst = new DenseInstance(stream.nextInstance()); 
				streamKM.trainOnInstanceImpl(trainInst);
				cnt ++;
				if (cnt % 10000 == 0) {
					LOG.info(cnt + " processed");
				}
			}
			
			double[][] centers = new double[clusterNums][dimension];
			for (int i = 0; i < clusterNums; i ++) {
				centers[i] = streamKM.getClusteringResult().get(i).getCenter();
			}
			
			try {
				ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath + "/" + inputFile.getName()));
				oos.writeObject(centers);
			} catch (FileNotFoundException e) {
				e.printStackTrace();  
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOG.info("Total " + cnt + " processed");
	}
}
