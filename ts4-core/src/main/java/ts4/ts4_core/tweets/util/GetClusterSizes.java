/* Get cluster sizes for batch daily/hourly and streaming hourly
 * 
 * Run: sh target/appassembler/bin/GetClusterSizes_2013 -docsvector {docVectorPath} 
 * -kmeansclusters {kmeansClustersPath} -streamingclusters {streamingClustersPath} 
 * -dimension {dimension} -partition {partitionNum} -output {outputPath}
 */
package ts4.ts4_core.tweets.util;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class GetClusterSizes {
	private static final String DOCVECTORS = "docsvector";
	private static final String KMEANS_CLUSTER_OPTION = "kmeansclusters";
	private static final String STREAMING_CLUSTER_OPTION = "streamingclusters";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "unchecked", "resource" })
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("document vector").create(DOCVECTORS));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("kmeans cluster centers path").create(KMEANS_CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("streaming cluster centers path").create(STREAMING_CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition").create(PARTITION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("output location").create(OUTPUT_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(DOCVECTORS) || !cmdline.hasOption(KMEANS_CLUSTER_OPTION) || !cmdline.hasOption(STREAMING_CLUSTER_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(GetClusterSizes.class.getName(), options);
			System.exit(-1);
		}

		String docVectorPath = cmdline.getOptionValue(DOCVECTORS);
		String kmeansClusterPath = cmdline.getOptionValue(KMEANS_CLUSTER_OPTION);
		String streamingClusterPath = cmdline.getOptionValue(STREAMING_CLUSTER_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

		BufferedWriter bw_batch_daily = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/batch-cluster-size-perday.txt", true)));
		BufferedWriter bw_batch_hourly = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/batch-cluster-size-perhour.txt", true)));
		BufferedWriter bw_online_hourly = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/streaming-cluster-size-perhour.txt", true)));

		// Batch daily
		double[][] centers_days = new double[partitionNum][dimension];
		List<Integer>[] indexes_days = (ArrayList<Integer>[])new ArrayList[partitionNum];
		for (int i = 0; i < partitionNum; i ++) {
			indexes_days[i] = new ArrayList<Integer>();
		}
		
		int ind = 0;
		try {
			File[] files = new File(kmeansClusterPath + "/clustercenters-d" + dimension + "-day1-trial1").listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
					String line;
					while((line = br.readLine()) != null) {
						line = line.substring(1, line.length() - 1);
						int i = 0;
						String[] vector = line.split(",");
						for (String coor : vector) {
							centers_days[ind][i ++] = Double.parseDouble(coor);
						}
						ind ++;
					}
					br.close();
				}
			}
		} catch(Exception e){
			System.out.println("File not found");
		}
		
		try {
			File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-day1-trial1").listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
					String line;
					while((line = br.readLine()) != null) {
						line = line.substring(1, line.length() - 1);
						String[] indexmap = line.split(",");
						indexes_days[Integer.parseInt(indexmap[1])].add(Integer.parseInt(indexmap[0]));
					}
					br.close();
				}
			}
		} catch(Exception e){
			System.out.println("File not found");
		}

		for (int i = 0; i < partitionNum; i ++) {
			bw_batch_daily.write(String.valueOf(indexes_days[i].size()));
			bw_batch_daily.newLine();
		}

		// Batch hourly
		double[][] centers_hours = new double[partitionNum][dimension];
		List<Integer>[] indexes_hours = (ArrayList<Integer>[])new ArrayList[partitionNum];
		for (int i = 0; i < partitionNum; i ++) {
			indexes_hours[i] = new ArrayList<Integer>();
		}

		ind = 0;
		try {
			File[] files = new File(kmeansClusterPath + "/clustercenters-d" + dimension + "-hour1-trial1").listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
					String line;
					while((line = br.readLine()) != null) {
						line = line.substring(1, line.length() - 1);
						int j = 0;
						String[] vector = line.split(",");
						for (String coor : vector) {
							centers_hours[ind][j ++] = Double.parseDouble(coor);
						}
						ind ++;
					}
					br.close();
				}
			}
		} catch(Exception e){
			System.out.println("File not found");
		}
		
		try {
			File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-hour1-trial1").listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
					String line;
					while((line = br.readLine()) != null) {
						line = line.substring(1, line.length() - 1);
						String[] indexmap = line.split(",");
						indexes_hours[Integer.parseInt(indexmap[1])].add(Integer.parseInt(indexmap[0]));
					}
					br.close();
				}
			}
		} catch(Exception e){
			System.out.println("File not found");
		}

		for (int i = 0; i < partitionNum; i ++) {
			bw_batch_hourly.write(String.valueOf(indexes_hours[i].size()));
			bw_batch_hourly.newLine();
		}

		// Online hourly
		List<double[]> docVector = new ArrayList<double[]>();
		File firstFile = new File(docVectorPath).listFiles()[0];
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(docVectorPath + "/" + firstFile.getName())));
			String line;
			while((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				double[] vector = new double[dimension];
				for (int j = 1; j < tokens.length; j ++) {
					vector[j - 1] = Double.parseDouble(tokens[j]);
				}
				docVector.add(vector);
			}
			br.close();
		} catch(Exception e){
			System.out.println("File not found");
		}

		for (int i = 0; i < partitionNum; i ++) {
			indexes_hours[i] = new ArrayList<Integer>();
		}
		ind = 0;
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(streamingClusterPath + "/" + firstFile.getName()));
			centers_hours = (double[][])ois.readObject();
		} catch (FileNotFoundException e) {
			e.printStackTrace();  
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < docVector.size(); i ++) {
			int nearestCluster = FindNearestCluster(docVector.get(i), centers_hours);
			indexes_hours[nearestCluster].add(ind);
			ind ++;
		}

		for (int i = 0; i < partitionNum; i ++) {
			bw_online_hourly.write(String.valueOf(indexes_hours[i].size()));
			bw_online_hourly.newLine();
		}

		bw_batch_daily.close();
		bw_batch_hourly.close();
		bw_online_hourly.close();		
	}

	public static int FindNearestCluster(double[] docVector, double[][] centers) {
		int res = 0;
		double max = Integer.MIN_VALUE;
		for (int i = 0; i < centers.length; i ++) {
			double similarity = 0;
			double docLength = 0;
			double centerLength = 0;
			double sum = 0;
			for (int j = 0; j < docVector.length; j ++) {
				docLength += docVector[j] * docVector[j];
				centerLength += centers[i][j] * centers[i][j];
				sum += docVector[j] * centers[i][j];
			}
			similarity = sum / (Math.sqrt(docLength) * Math.sqrt(centerLength));
			if (similarity > max) {
				max = similarity;
				res = i;
			}
		}
		return res;
	}
}
