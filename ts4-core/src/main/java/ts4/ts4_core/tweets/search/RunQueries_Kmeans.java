/* Run queries on kmeans results daily with cf up till the query time
 * Run: sh target/appassembler/bin/RunQueries_Kmeans -index {indexPath} -stats {statsPath} -cf {queryCfPath}
 *   	-kmeansclusters {kmeansclustersPath} -dayhours {dayFile} [-hourly true] -dimension {dimension} -partition {partitionNum} [-top {N}]
 * 		-queries {queriesPath} -queriesvector {queryVectorPath} -trial {trial} -output {outputPath}
 */
package ts4.ts4_core.tweets.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import ts4.ts4_core.tweets.util.TermStatistics;
import ts4.ts4_core.tweets.util.TweetParser;

public class RunQueries_Kmeans {
	private static final Logger LOG = Logger.getLogger(RunQueries_Kmeans.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";
	private static final String STATS_OPTION = "stats";
	private static final String CF_OPTION = "cf";
	private static final String KMEANS_CLUSTER_OPTION = "kmeansclusters";
	private static final String DAYHOURS_OPTION = "dayhours";
	private static final String HOURS_OPTION = "hourly";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String TOP = "top";
	private static final String QUERIES_OPTION = "queries";
	private static final String QUERIES_VECTOR_OPTION = "queriesvector";
	private static final String TRIAL = "trial";
	private static final String OUTPUT_OPTION = "output";

	@SuppressWarnings({ "static-access", "unchecked" })
	public static void main(String[] args) throws Exception {
		float mu = 2500.0f;
		int numResults = 1000;

		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("statistics location").create(STATS_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("query cf file").create(CF_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("kmeans cluster centers path").create(KMEANS_CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("dayhours file").create(DAYHOURS_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("whether hourly").create(HOURS_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition").create(PARTITION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("top k").create(TOP));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("query vector").create(QUERIES_VECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("trial").create(TRIAL));
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

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(STATS_OPTION) || !cmdline.hasOption(CF_OPTION) || !cmdline.hasOption(KMEANS_CLUSTER_OPTION) || !cmdline.hasOption(DAYHOURS_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(QUERIES_VECTOR_OPTION) || !cmdline.hasOption(TRIAL) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueries_Kmeans.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		String statsPath = cmdline.getOptionValue(STATS_OPTION);
		String cfPath = cmdline.getOptionValue(CF_OPTION);
		String kmeansClusterPath = cmdline.getOptionValue(KMEANS_CLUSTER_OPTION);
		String dayhoursPath = cmdline.getOptionValue(DAYHOURS_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		int top = cmdline.hasOption(TOP) ? Integer.parseInt(cmdline.getOptionValue(TOP)) : partitionNum;
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String queryVectorPath = cmdline.getOptionValue(QUERIES_VECTOR_OPTION);
		String trial = cmdline.getOptionValue(TRIAL);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

		// Read in index
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
		LOG.info("Reading term statistics from index");
		TermStatistics termStats = new TermStatistics(indexPath);
		LOG.info("Finished reading term statistics from index");

		// Read in stats
		LOG.info("Reading term statistics from file");
		BufferedReader br_stats = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/stats.txt")));
		int numDoc = Integer.parseInt(br_stats.readLine());
		br_stats.close();

		int[][] terms = new int[numDoc][];
		int[][] tf = new int[numDoc][];
		long[] ids = new long[numDoc];
		int[] docLengthOrdered = new int[numDoc];
		float[] docLengthEncoded = new float[numDoc];
		try {
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/doc_id.txt")));
			String s;
			int i = 0;
			while((s = brTerms.readLine()) != null) {
				ids[i ++] = Long.valueOf(s);
			}
			try {
				brTerms.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedReader brLength = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/doc_length_ordered.txt")));
			String s;
			int i = 0;
			while((s = brLength.readLine()) != null) {
				docLengthOrdered[i ++] = Integer.valueOf(s);
			}
			try {
				brLength.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedReader brLength = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/doc_length_encoded.txt")));
			String s;
			int i = 0;
			while((s = brLength.readLine()) != null) {
				docLengthEncoded[i ++] = Float.valueOf(s);
			}
			try {
				brLength.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/all_terms_ordered.txt")));
			String s;
			int i = 0;
			int j = 0;
			terms[0] = new int[docLengthOrdered[0]];
			while((s = brTerms.readLine()) != null) {
				while (j == docLengthOrdered[i] || docLengthOrdered[i] == 0) {
					i ++;
					j = 0;
					terms[i] = new int[docLengthOrdered[i]];
				}
				terms[i][j ++] = Integer.valueOf(s);
			}
			try {
				brTerms.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(new FileInputStream(statsPath + "/all_terms_tf.txt")));
			String s;
			int i = 0;
			int j = 0;
			tf[0] = new int[docLengthOrdered[0]];
			while((s = brTerms.readLine()) != null) {
				while (j == docLengthOrdered[i] || docLengthOrdered[i] == 0) {
					i ++;
					j = 0;
					tf[i] = new int[docLengthOrdered[i]];
				}
				tf[i][j ++] = Integer.valueOf(s);
			}
			try {
				brTerms.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		LOG.info("Finished reading term statistics from file");

		// Read in cf file
		int queryCount = 0;
		List<List<Long>> cf = new ArrayList<List<Long>>();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(cfPath)));
			String line;
			while((line = br.readLine()) != null) {
				cf.add(new ArrayList<Long>());
				String[] tokens = line.split(" ");
				for (String token : tokens) {
					cf.get(queryCount).add(Long.parseLong(token));
				}
				queryCount ++;
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

		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for (@SuppressWarnings("unused") TrecTopic topic : topics) {
			topicTotal ++;
		}

		// Read in dayhours File
		int[] days = new int[topicTotal];
		int[] hours = new int[topicTotal];
		int cnt = 0;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(dayhoursPath)));
			String line;
			while((line = br.readLine()) != null) {
				String[] tokens = line.split(" ");
				if (!cmdline.hasOption(HOURS_OPTION)) {
					days[cnt] = Integer.parseInt(tokens[0]);
					hours[cnt ++] = Integer.parseInt(tokens[1]);
				} else {
					hours[cnt ++] = 24 * Integer.parseInt(tokens[0]) + Integer.parseInt(tokens[1]);
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
		
		// Count days
		int DAYS = 0;
		try {
			File[] files = new File(kmeansClusterPath).listFiles();
			for (File file : files) {
				if (file.getName().startsWith("clustercenters-d" + dimension + "-hour") && file.getName().endsWith("trial1")) {
					DAYS ++;
				}
			}
		} catch(Exception e){
			System.out.println("File not found");
		}
		DAYS /= 24;

		// Read in cluster centers and assignments
		LOG.info("Reading cluster centers and assignments from file");
		double[][][] centers_days = new double[DAYS][partitionNum][dimension];
		List<Integer>[][] indexes_days = (ArrayList<Integer>[][])new ArrayList[DAYS][partitionNum];
		if (!cmdline.hasOption(HOURS_OPTION)) {
			for (int i = 0; i < DAYS; i ++) {
				for (int j = 0; j < partitionNum; j ++) {
					indexes_days[i][j] = new ArrayList<Integer>();
				}
			}
			for (int i = 1; i <= DAYS; i ++) {
				int ind = 0;
				try {
					File[] files = new File(kmeansClusterPath + "/clustercenters-d" + dimension + "-day" + i + "-trial" + trial).listFiles();
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
									centers_days[i - 1][ind][j ++] = Double.parseDouble(coor);
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
					File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-day" + i + "-trial" + trial).listFiles();
					Arrays.sort(files);
					for (File file : files) {
						if (file.getName().startsWith("part")) {
							BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
							String line;
							while((line = br.readLine()) != null) {
								line = line.substring(1, line.length() - 1);
								String[] indexmap = line.split(",");
								indexes_days[i - 1][Integer.parseInt(indexmap[1])].add(Integer.parseInt(indexmap[0]));
							}
							br.close();
						}
					}
				} catch(Exception e){
					System.out.println("File not found");
				}
			}
		}

		double[][][] centers_hours = new double[DAYS * 24][partitionNum][dimension];
		List<Integer>[][] indexes_hours = (ArrayList<Integer>[][])new ArrayList[DAYS * 24][partitionNum];
		for (int i = 0; i < DAYS * 24; i ++) {
			for (int j = 0; j < partitionNum; j ++) {
				indexes_hours[i][j] = new ArrayList<Integer>();
			}
		}
		for (int i = 1; i <= DAYS * 24; i ++) {
			int ind = 0;
			try {
				File[] files = new File(kmeansClusterPath + "/clustercenters-d" + dimension + "-hour" + i + "-trial" + trial).listFiles();
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
								centers_hours[i - 1][ind][j ++] = Double.parseDouble(coor);
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
				File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-hour" + i + "-trial" + trial).listFiles();
				Arrays.sort(files);
				for (File file : files) {
					if (file.getName().startsWith("part")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
						String line;
						while((line = br.readLine()) != null) {
							line = line.substring(1, line.length() - 1);
							String[] indexmap = line.split(",");
							indexes_hours[i - 1][Integer.parseInt(indexmap[1])].add(Integer.parseInt(indexmap[0]));
						}
						br.close();
					}
				}
			} catch(Exception e){
				System.out.println("File not found");
			}
		}
		LOG.info("Finished reading cluster centers and assignments from file");

		LOG.info("Running queries");
		double[][] queryVector = new double[topicTotal][dimension];
		int ind = 0;
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(queryVectorPath)));
			String line;
			while((line = br.readLine()) != null) {
				String[] vector = line.split(" ");
				for (int i = 0; i < dimension; i ++) {
					queryVector[ind][i] = Double.parseDouble(vector[i + 1]);
				}
				ind ++;
			}
			br.close();
		} catch(Exception e){
			System.out.println("File not found");
		}

		int topicCnt = 0;
		double[] percentage = new double[partitionNum];
		for (int topIdx = 1; topIdx <= top; topIdx ++) {
			if (cmdline.hasOption(HOURS_OPTION)) {
				File f = new File(outputPath + "/glove_d" + dimension + "_mean_hourly_top" + topIdx + "_trial" + trial + ".txt");
				if (f.exists()) {
					f.delete();
				}
			} else {
				File f = new File(outputPath + "/glove_d" + dimension + "_mean_daily_top" + topIdx + "_trial" + trial + ".txt");
				if (f.exists()) {
					f.delete();
				}
			}
		}
		for (TrecTopic topic : topics) {
			List<String> queryterms = TweetParser.parse(ANALYZER, topic.getQuery());
			int[] qids = new int[queryterms.size()];
			int c = 0;
			for (String term : queryterms) {
				qids[c] = termStats.getId(term);
				c++;
			}

			int[][] partitions = new int[days[topicCnt] + hours[topicCnt]][partitionNum];
			int partitionInd = 0;
			for (int day = 1; day <= days[topicCnt]; day ++) {
				partitions[partitionInd ++] = determinePartition(centers_days[day - 1], queryVector[topicCnt], 100);
			}
			for (int hour = 24 * days[topicCnt] + 1; hour <= 24 * days[topicCnt] + hours[topicCnt]; hour ++) {
				partitions[partitionInd ++] = determinePartition(centers_hours[hour - 1], queryVector[topicCnt], 100);
			}

			int[] selectedSizeArr = new int[partitionNum];
			int selectedSize = 0;
			TopNScoredLongs topN = new TopNScoredLongs(numResults);
			TopNScoredLongs prevTopN = new TopNScoredLongs(numResults);
			for (int topIdx = 1; topIdx <= top; topIdx ++) {
				BufferedWriter bw = null;
				if (cmdline.hasOption(HOURS_OPTION)) {
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_hourly_top" + topIdx + "_trial" + trial + ".txt", true)));
				} else {
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_daily_top" + topIdx + "_trial" + trial + ".txt", true)));
				}
				partitionInd = 0;
				for (int day = 1; day <= days[topicCnt]; day ++) {
					for (int idx = 0; idx < indexes_days[day - 1][partitions[partitionInd][topIdx - 1]].size(); idx ++) {
						int i = indexes_days[day - 1][partitions[partitionInd][topIdx - 1]].get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
							for (int j = 0; j < docLengthOrdered[i]; j ++) {
								if (terms[i][j] == qids[t]) {
									score += Math.log(1 + tf[i][j] / (mu * prob));
									score += Math.log(mu / (docLengthEncoded[i] + mu));
									break;
								}
							}
						}
						if (score > 0) {
							prevTopN.add(ids[i], score);
						}
					}
					partitionInd ++;
				}
				for (int hour = 24 * days[topicCnt] + 1; hour <= 24 * days[topicCnt] + hours[topicCnt] - 1; hour ++) {
					for (int idx = 0; idx < indexes_hours[hour - 1][partitions[partitionInd][topIdx - 1]].size(); idx ++) {
						int i = indexes_hours[hour - 1][partitions[partitionInd][topIdx - 1]].get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
							for (int j = 0; j < docLengthOrdered[i]; j ++) {
								if (terms[i][j] == qids[t]) {
									score += Math.log(1 + tf[i][j] / (mu * prob));
									score += Math.log(mu / (docLengthEncoded[i] + mu));
									break;
								}
							}
						}
						if (score > 0) {
							prevTopN.add(ids[i], score);
						}
					}
					partitionInd ++;
				}
				TopNScoredLongs tempTopN = new TopNScoredLongs(numResults);
				for (PairOfLongFloat pair : prevTopN.extractAll()) {
					tempTopN.add(pair.getKey(), pair.getValue());
					topN.add(pair.getKey(), pair.getValue());
				}
				int finalHour = 24 * days[topicCnt] + hours[topicCnt];
				for (int partition = 0; partition < partitionNum; partition ++) {
					for (int idx = 0; idx < indexes_hours[finalHour - 1][partition].size(); idx ++) {
						int i = indexes_hours[finalHour - 1][partition].get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
							for (int j = 0; j < docLengthOrdered[i]; j ++) {
								if (terms[i][j] == qids[t]) {
									score += Math.log(1 + tf[i][j] / (mu * prob));
									score += Math.log(mu / (docLengthEncoded[i] + mu));
									break;
								}
							}
						}
						if (score > 0) {
							topN.add(ids[i], score);
						}
					}
				}
				selectedSizeArr[topIdx - 1] = selectedSize;

				int count = 1;
				for (PairOfLongFloat pair : topN.extractAll()) {
					bw.write(String.format("%d Q0 %s %d %f kmeans", Integer.parseInt(topic.getId().substring(2)), pair.getKey(), count, pair.getValue()));
					bw.newLine();
					count ++;
				}
				prevTopN = tempTopN;
				bw.close();
			}
			topicCnt ++;
			for (int topIdx = 1; topIdx <= top; topIdx ++) {
				percentage[topIdx - 1] += (double)(selectedSizeArr[topIdx - 1]) / selectedSizeArr[partitionNum - 1];
			}
		}
		for (int topIdx = 1; topIdx <= top; topIdx ++) {
			System.out.println(topIdx + "\t" + (percentage[topIdx - 1] / topicCnt));
		}
	}

	public static int[] determinePartition(double[][] centers, double[] queryVector, int top) {
		List<ScoreIdPair> all = new ArrayList<ScoreIdPair>();
		// Cosine similarity
		for (int i = 0; i < centers.length; i ++) {
			double similarity = 0;
			double centerLength = 0;
			double queryLength = 0;
			double sum = 0;
			for (int j = 0; j < queryVector.length; j ++) {
				centerLength += centers[i][j] * centers[i][j];
				queryLength += queryVector[j] * queryVector[j];
				sum += centers[i][j] * queryVector[j];
			}
			similarity = sum / (Math.sqrt(centerLength) * Math.sqrt(queryLength));
			all.add(new ScoreIdPair(similarity, i));
		}
		Collections.sort(all, new ScoreComparator());

		int[] result = new int[top];
		int count = 0;
		for (ScoreIdPair pair : all) {
			if (count < top) {
				result[count] = pair.getIndex();
				count ++;
			} else {
				break;
			}
		}
		return result;
	}
}
