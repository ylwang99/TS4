/* Run queries on kmeans results daily with cf up till the query time
 * Run: sh target/appassembler/bin/RunQueriesDaily_CFPerQuery -index {indexPath} -stats {statsPath} -cf {queryCfPath}
 *   	-clusters {clustersPath} -dayhours {dayFile} [-hourly true] -dimension {dimension} -partition {partitionNum} -top {N}
 * 		-queries {queriesPath} -queriesvector {queryVectorPath} -output {outputPath} -trail {trail}
 */
package ts4.ts4_core.tweets.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import ts4.ts4_core.tweets.util.*;

import com.google.common.collect.Lists;

import edu.umd.cloud9.io.pair.PairOfIntFloat;
import edu.umd.cloud9.util.TopNScoredInts;

public class RunQueriesDaily_CFPerQuery_SpecialStore {
	private static final Logger LOG = Logger.getLogger(RunQueriesDaily_CFPerQuery_SpecialStore.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);
	private static final int DAYS = 17;

	private static final String INDEX_OPTION = "index";
	private static final String STATS_OPTION = "stats";
	private static final String CF_OPTION = "cf";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String CLUSTER_OPTION = "clusters";
	private static final String DAYHOURS_OPTION = "dayhours";
	private static final String HOURS_OPTION = "hourly";
	private static final String TOP = "top";
	private static final String QUERIES_OPTION = "queries";
	private static final String QUERIES_VECTOR_OPTION = "queriesvector";
	private static final String OUTPUT_OPTION = "output";
	private static final String TRAIL = "trail";

	@SuppressWarnings({ "static-access" })
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
				.withDescription("cluster centers path").create(CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition").create(PARTITION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("dayhours file").create(DAYHOURS_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("whether hourly").create(HOURS_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("top k").create(TOP));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("query vector").create(QUERIES_VECTOR_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("output location").create(OUTPUT_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("trail").create(TRAIL));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(TRAIL) || !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(STATS_OPTION) || !cmdline.hasOption(CF_OPTION) || !cmdline.hasOption(CLUSTER_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(DAYHOURS_OPTION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(QUERIES_VECTOR_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueriesDaily_CFPerQuery_SpecialStore.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		String statsPath = cmdline.getOptionValue(STATS_OPTION);
		String cfPath = cmdline.getOptionValue(CF_OPTION);
		String clusterPath = cmdline.getOptionValue(CLUSTER_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		int top = cmdline.hasOption(TOP) ? Integer.parseInt(cmdline.getOptionValue(TOP)) : 1;
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String queryVectorPath = cmdline.getOptionValue(QUERIES_VECTOR_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		String trail = cmdline.getOptionValue(TRAIL);
		
		// Read in index
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
		LOG.info("Reading term statistics from index.");
		TermStatistics termStats = new TermStatistics(indexPath, GenerateStatistics.NUM_DOCS);
		LOG.info("Finished reading term statistics from index.");

		// Read in stats
		LOG.info("Reading term statistics from file.");
		int totalTerms = 0;
		int numDoc = 0;
		try {
			FileInputStream fisTerms = new FileInputStream(statsPath + "/all_terms_ordered.txt");
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(fisTerms));
			while((brTerms.readLine()) != null) {
				totalTerms ++;
			}
			try {
				fisTerms.close();
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
			FileInputStream fisLength = new FileInputStream(statsPath + "/doc_length_ordered.txt");
			BufferedReader brLength = new BufferedReader(new InputStreamReader(fisLength));
			while((brLength.readLine()) != null) {
				numDoc ++;
			}
			try {
				fisLength.close();
				brLength.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		int[] terms = new int[totalTerms];
		int[] tf = new int[totalTerms];
		long[] ids = new long[numDoc];
		int[] docLengthOrdered = new int[numDoc];
		float[] docLengthEncoded = new float[numDoc];
		int[] offsets = new int[numDoc];
		try {
			FileInputStream fisTerms = new FileInputStream(statsPath + "/all_terms_ordered.txt");
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(fisTerms));
			String s;
			int i = 0;
			while((s = brTerms.readLine()) != null) {
				terms[i ++] = Integer.valueOf(s);
			}
			try {
				fisTerms.close();
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
			FileInputStream fisTerms = new FileInputStream(statsPath + "/all_terms_tf.txt");
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(fisTerms));
			String s;
			int i = 0;
			while((s = brTerms.readLine()) != null) {
				tf[i ++] = Integer.valueOf(s);
			}
			try {
				fisTerms.close();
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
			FileInputStream fisTerms = new FileInputStream(statsPath + "/doc_id.txt");
			BufferedReader brTerms = new BufferedReader(new InputStreamReader(fisTerms));
			String s;
			int i = 0;
			while((s = brTerms.readLine()) != null) {
				ids[i ++] = Long.valueOf(s);
			}
			try {
				fisTerms.close();
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
			FileInputStream fisLength = new FileInputStream(statsPath + "/doc_length_ordered.txt");
			BufferedReader brLength = new BufferedReader(new InputStreamReader(fisLength));
			String s;
			int i = 0;
			while((s = brLength.readLine()) != null) {
				docLengthOrdered[i ++] = Integer.valueOf(s);
			}
			try {
				fisLength.close();
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
			FileInputStream fisLength = new FileInputStream(statsPath + "/doc_length_encoded.txt");
			BufferedReader brLength = new BufferedReader(new InputStreamReader(fisLength));
			String s;
			int i = 0;
			while((s = brLength.readLine()) != null) {
				docLengthEncoded[i ++] = Float.valueOf(s);
			}
			try {
				fisLength.close();
				brLength.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (int i = 1; i < numDoc; i ++) {
			offsets[i] = offsets[i - 1] + docLengthOrdered[i - 1];
		}
		LOG.info("Finished reading term statistics from file.");
		
		// Read in cluster centers and assignments
		LOG.info("Reading cluster centers and assignments from file.");
		List<double[][]> centers_days = new ArrayList<double[][]>();
		List<List<List<Integer>>> indexes_days = new ArrayList<List<List<Integer>>>();
		for (int i = 1; i <= DAYS; i ++) {
			int ind = 0;
			centers_days.add(new double[partitionNum][dimension]);
			indexes_days.add(new ArrayList<List<Integer>>());
			for (int j = 0; j < partitionNum; j ++) {
				indexes_days.get(indexes_days.size() - 1).add(new ArrayList<Integer>());
			}
			try {
				File[] files = new File(clusterPath + "/clustercenters-d" + dimension + "-day" + i + "-2011-trail" + trail).listFiles();
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
	        					centers_days.get(centers_days.size() - 1)[ind][j ++] = Double.parseDouble(coor);
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
				File[] files = new File(clusterPath + "/clusterassign-d" + dimension + "-day" + i + "-2011-trail" + trail).listFiles();
				Arrays.sort(files);
				for (File file : files) {
					if (file.getName().startsWith("part")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
	                    String line;
	                    while((line = br.readLine()) != null) {
	                    	line = line.substring(1, line.length() - 1);
	                    	String[] indexmap = line.split(",");
	                    	indexes_days.get(indexes_days.size() - 1).get(Integer.parseInt(indexmap[1])).add(Integer.parseInt(indexmap[0]));
	        			}
	                    br.close();
					}
				}
			} catch(Exception e){
	            System.out.println("File not found");
			}
		}
		List<double[][]> centers_hours = new ArrayList<double[][]>();
		List<List<List<Integer>>> indexes_hours = new ArrayList<List<List<Integer>>>();
		for (int i = 1; i <= 24 * DAYS; i ++) {
			int ind = 0;
			centers_hours.add(new double[partitionNum][dimension]);
			indexes_hours.add(new ArrayList<List<Integer>>());
			for (int j = 0; j < partitionNum; j ++) {
				indexes_hours.get(indexes_hours.size() - 1).add(new ArrayList<Integer>());
			}
			try {
				File[] files = new File(clusterPath + "/clustercenters-d" + dimension + "-hour" + i + "-2011-trail" + trail).listFiles();
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
	        					centers_hours.get(centers_hours.size() - 1)[ind][j ++] = Double.parseDouble(coor);
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
				File[] files = new File(clusterPath + "/clusterassign-d" + dimension + "-hour" + i + "-2011-trail" + trail).listFiles();
				Arrays.sort(files);
				for (File file : files) {
					if (file.getName().startsWith("part")) {
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
	                    String line;
	                    while((line = br.readLine()) != null) {
	                    	line = line.substring(1, line.length() - 1);
	                    	String[] indexmap = line.split(",");
	                    	indexes_hours.get(indexes_hours.size() - 1).get(Integer.parseInt(indexmap[1])).add(Integer.parseInt(indexmap[0]));
	        			}
	                    br.close();
					}
				}
			} catch(Exception e){
	            System.out.println("File not found");
			}
		}
//		for (int i = 0; i < partitionNum; i ++) {
//			System.out.println(indexes_hours.get(0).get(i).size());
//		}
		LOG.info("Finished reading cluster centers and assignments from file.");
		
		// Read in cf file
		int queryCount = 0;
		List<List<Integer>> cf = new ArrayList<List<Integer>>();
		try {
			FileInputStream fis = new FileInputStream(cfPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line;
			while((line = br.readLine()) != null) {
				cf.add(new ArrayList<Integer>());
				String[] tokens = line.split(" ");
				for (String token : tokens) {
					cf.get(queryCount).add(Integer.parseInt(token));
				}
				queryCount ++;
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
		
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for ( @SuppressWarnings("unused") TrecTopic topic : topics ) {
			topicTotal ++;
		}
		
		// Read in dayhours File
		int[] days = new int[topicTotal];
		int[] hours = new int[topicTotal];
		int cnt = 0;
		String dayhoursPath = cmdline.getOptionValue(DAYHOURS_OPTION);
		try {
			FileInputStream fis = new FileInputStream(dayhoursPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
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
		
		LOG.info("Running queries.");
//		PrintStream out = new PrintStream(System.out, true, "UTF-8");
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
		
//		System.out.println("top n\tavg scan size percentage");			
		int topicCnt = 0;
		double[] percentage = new double[partitionNum];
		for (top = 1; top <= partitionNum; top ++) {
			if (cmdline.hasOption(HOURS_OPTION)) {
				File f = new File(outputPath + "/glove_d" + dimension + "_mean_hourly_top" + top + "_trail" + trail + ".txt");
				if (f.exists()) {
					f.delete();
				}
			} else {
				File f = new File(outputPath + "/glove_d" + dimension + "_mean_daily_top" + top + "_trail" + trail + ".txt");
				if (f.exists()) {
					f.delete();
				}
			}
		}
		for ( TrecTopic topic : topics ) {
			List<String> queryterms = parse(ANALYZER, topic.getQuery());
			int[] qids = new int[queryterms.size()];
			int c = 0;
			for (String term : queryterms) {
				qids[c] = termStats.getId(term);
				c++;
			}
			
			int[][] partitions = new int[days[topicCnt] + hours[topicCnt]][partitionNum];
			int partitionInd = 0;
			for (int day = 1; day <= days[topicCnt]; day ++) {
				partitions[partitionInd ++] = determinePartition(centers_days.get(day - 1), queryVector[topicCnt], 100);
			}
			for (int hour = 24 * days[topicCnt] + 1; hour <= 24 * days[topicCnt] + hours[topicCnt]; hour ++) {
				partitions[partitionInd ++] = determinePartition(centers_hours.get(hour - 1), queryVector[topicCnt], 100);
			}
			
			int[] selectedSizeArr = new int[partitionNum];
			int selectedSize = 0;
			TopNScoredInts topN = new TopNScoredInts(numResults);
			for (top = 1; top <= partitionNum; top ++) {
				BufferedWriter bw = null;
				if (cmdline.hasOption(HOURS_OPTION)) {
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_hourly_top" + top + "_trail" + trail + ".txt", true)));
				} else {
					bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_daily_top" + top + "_trail" + trail + ".txt", true)));
				}
				partitionInd = 0;
				for (int day = 1; day <= days[topicCnt]; day ++) {
					for (int idx = 0; idx < indexes_days.get(day - 1).get(partitions[partitionInd][top - 1]).size(); idx ++) {
						int i = indexes_days.get(day - 1).get(partitions[partitionInd][top - 1]).get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
							for (int j = 0; j < docLengthOrdered[i]; j ++) {
								if (terms[offsets[i] + j] == qids[t]) {
									score += Math.log(1 + tf[offsets[i] + j] / (mu * prob));
									score += Math.log(mu / (docLengthEncoded[i] + mu));
									break;
								}
							}
						}
						if (score > 0) {
							topN.add(i, score);
						}
					}
					partitionInd ++;
				}
				for (int hour = 24 * days[topicCnt] + 1; hour <= 24 * days[topicCnt] + hours[topicCnt] - 1; hour ++) {
					for (int idx = 0; idx < indexes_hours.get(hour - 1).get(partitions[partitionInd][top - 1]).size(); idx ++) {
						int i = indexes_hours.get(hour - 1).get(partitions[partitionInd][top - 1]).get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
							for (int j = 0; j < docLengthOrdered[i]; j ++) {
								if (terms[offsets[i] + j] == qids[t]) {
									score += Math.log(1 + tf[offsets[i] + j] / (mu * prob));
									score += Math.log(mu / (docLengthEncoded[i] + mu));
									break;
								}
							}
						}
						if (score > 0) {
							topN.add(i, score);
						}
					}
					partitionInd ++;
				}
				if (top == 1) {
					int finalHour = 24 * days[topicCnt] + hours[topicCnt];
					for (int partition = 0; partition < partitionNum; partition ++) {
						for (int idx = 0; idx < indexes_hours.get(finalHour - 1).get(partition).size(); idx ++) {
							int i = indexes_hours.get(finalHour - 1).get(partition).get(idx);
							if (ids[i] > topic.getQueryTweetTime()) {
								continue;
							}
							selectedSize ++;
							float score = 0.0F;
							for (int t = 0; t < c; t++) {
								float prob = (float)(cf.get(topicCnt).get(t) + 1) / (cf.get(topicCnt).get(c) + 1);
								for (int j = 0; j < docLengthOrdered[i]; j ++) {
									if (terms[offsets[i] + j] == qids[t]) {
										score += Math.log(1 + tf[offsets[i] + j] / (mu * prob));
										score += Math.log(mu / (docLengthEncoded[i] + mu));
										break;
									}
								}
							}
							if (score > 0) {
								topN.add(i, score);
							}
						}
					}
				}
				selectedSizeArr[top - 1] = selectedSize;
			
				int count = 1;
				TopNScoredInts tempTopN = new TopNScoredInts(numResults);
				for (PairOfIntFloat pair : topN.extractAll()) {
					tempTopN.add(pair.getKey(), pair.getValue());
					bw.write(String.format("%d Q0 %s %d %f kmeans", Integer.parseInt(topic.getId().substring(2)), ids[pair.getKey()], count, pair.getValue()));
					bw.newLine();
					count ++;
				}
				topN = tempTopN;
				bw.close();
			}
			topicCnt ++;
			for (top = 1; top <= partitionNum; top ++) {
				percentage[top - 1] += (double)(selectedSizeArr[top - 1]) / selectedSizeArr[partitionNum - 1];
			}
		}
		for (top = 1; top <= partitionNum; top ++) {
			System.out.println(top + "\t" + (percentage[top - 1] / topicCnt));
		}
	}

	public static List<String> parse(Analyzer analyzer, String s) throws IOException {
		List<String> list = Lists.newArrayList();

		TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));
		CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
		tokenStream.reset();
		while (tokenStream.incrementToken()) {
			list.add(cattr.toString());
		}
		tokenStream.end();
		tokenStream.close();

		return list;
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
