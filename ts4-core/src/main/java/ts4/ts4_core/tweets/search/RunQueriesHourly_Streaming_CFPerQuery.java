/* Run queries on streaming kmeans results hourly with cf up till the query time
 * Run: sh target/appassembler/bin/RunQueriesHourly_Streaming_CFPerQuery -index {indexPath} -stats {statsPath} -clusters {clustersPath}
 * 		-dayhours {dayFile} [-hourly true] -dimension {dimension} -partition {partitionNum} -top {N}
 * 		-queries {queriesPath} -queriesvector {queryVectorPath} -output {outputPath}
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
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;

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

public class RunQueriesHourly_Streaming_CFPerQuery {
	private static final Logger LOG = Logger.getLogger(RunQueriesHourly_Streaming_CFPerQuery.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";
	private static final String STATS_OPTION = "stats";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String DOCVECTORS = "docsvector";
	private static final String CLUSTER_OPTION = "clusters";
	private static final String DAYHOURS_OPTION = "dayhours";
	private static final String TOP = "top";
	private static final String QUERIES_OPTION = "queries";
	private static final String QUERIES_VECTOR_OPTION = "queriesvector";
	private static final String OUTPUT_OPTION = "output";

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
				.withDescription("cluster centers path").create(CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("document vector").create(DOCVECTORS));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition").create(PARTITION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("dayhours file").create(DAYHOURS_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("top k").create(TOP));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("file containing topics in TREC format").create(QUERIES_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("query vector").create(QUERIES_VECTOR_OPTION));
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

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(STATS_OPTION) || !cmdline.hasOption(CLUSTER_OPTION) || !cmdline.hasOption(DOCVECTORS) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(DAYHOURS_OPTION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(QUERIES_VECTOR_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueriesHourly_Streaming_CFPerQuery.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		String statsPath = cmdline.getOptionValue(STATS_OPTION);
		String clusterPath = cmdline.getOptionValue(CLUSTER_OPTION);
		String docVectorPath = cmdline.getOptionValue(DOCVECTORS); 
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		int top = cmdline.hasOption(TOP) ? Integer.parseInt(cmdline.getOptionValue(TOP)) : 1;
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String queryVectorPath = cmdline.getOptionValue(QUERIES_VECTOR_OPTION);
		String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
		
		// Read in index
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}
//		LOG.info("Reading term statistics from index.");
		TermStatistics termStats = new TermStatistics(indexPath, GenerateStatistics.NUM_DOCS);
//		LOG.info("Finished reading term statistics from index.");

		// Read in stats
//		LOG.info("Reading term statistics from file.");
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
//		LOG.info("Finished reading term statistics from file.");
		
		// Read in document vectors
//		LOG.info("Reading document vectors.");
		double[][] docVector = new double[numDoc][dimension];
		try {
			File[] files = new File(docVectorPath).listFiles();
			Arrays.sort(files);
			for (File file : files) {
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
                String line;
                while((line = br.readLine()) != null) {
    				String[] tokens = line.split(" ");
    				for (int j = 1; j < tokens.length; j ++) {
    					docVector[Integer.parseInt(tokens[0])][j - 1] = Double.parseDouble(tokens[j]);
    				}
    			}
                br.close();
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
//		LOG.info("Finished reading document vectors.");
		
		// Read in cluster assignments
//		LOG.info("Reading cluster centers and assignments from file.");
		List<double[][]> centers_hours = new ArrayList<double[][]>();
		List<List<List<Integer>>> indexes_hours = new ArrayList<List<List<Integer>>>();
		double[][] vectorSum = new double[partitionNum][dimension];
		int docCntHours = 0;
		Set<Integer> clusterindexes = new HashSet<Integer>();
		try {
			File[] files = new File(clusterPath).listFiles();
			Arrays.sort(files);
			for (File file : files) {
				File[] subFiles = new File(file.getPath()).listFiles();
				for (File subFile : subFiles) {
					if (subFile.getName().startsWith("part")) {
						centers_hours.add(new double[partitionNum][dimension]);
						indexes_hours.add(new ArrayList<List<Integer>>());
						for (int j = 0; j < partitionNum; j ++) {
							indexes_hours.get(indexes_hours.size() - 1).add(new ArrayList<Integer>());
						}
						BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(subFile.getPath())));
	                    String line;
	                    while((line = br.readLine()) != null) {
	                    	line = line.substring(1, line.length() - 1);
	                    	String[] indexmap = line.split(",");
	                    	int docidx = (int)(Float.parseFloat(indexmap[0]));
	                    	int clusteridx = Integer.parseInt(indexmap[1]);
	                    	clusterindexes.add(clusteridx);
	                    	indexes_hours.get(indexes_hours.size() - 1).get(clusteridx).add(docidx);
	                    	for (int d = 0; d < dimension; d ++) {
	                    		vectorSum[clusteridx][d] += docVector[docidx][d];
	                    	}
	                    	docCntHours ++;
	                    }
	                    for (int c = 0; c < partitionNum; c ++) {
		                    for (int d = 0; d < dimension; d ++) {
		                    	centers_hours.get(centers_hours.size() - 1)[c][d] = vectorSum[c][d] / docCntHours;
		                    }
	                    }
	                    br.close();
					}
				}
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
		
//		LOG.info("Finished reading cluster centers and assignments from file.");
		
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for ( @SuppressWarnings("unused") TrecTopic topic : topics ) {
			topicTotal ++;
		}
		
		CFStats[] cf = new CFStats[topicTotal];
		ObjectInputStream ois = null;
		try {
			File[] files = new File(statsPath + "/cf/query/").listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("query")) {
					ois = new ObjectInputStream(new FileInputStream(file.getPath()));
					cf[Integer.parseInt(file.getName().substring(5)) - 1] = (CFStats) ois.readObject();
				}
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
		ois.close();
		
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
				days[cnt] = Integer.parseInt(tokens[0]);
				hours[cnt ++] = Integer.parseInt(tokens[1]);
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
		
//		LOG.info("Running queries.");
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
		for (top = 1; top <= partitionNum; top ++) {
			float avgperctg = 0.0f;
			BufferedWriter bw = null;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_hourly_top" + top + ".txt")));
			int topicCnt = 0;
			for ( TrecTopic topic : topics ) {  
				List<String> queryterms = parse(ANALYZER, topic.getQuery());
				TopNScoredInts topN = new TopNScoredInts(numResults);
				int[] qids = new int[queryterms.size()];
				int c = 0;
				for (String term : queryterms) {
					qids[c] = termStats.getId(term);
					c++;
				}
				
				int totalSize = 0;
				for (int hour = 1; hour <= 24 * days[topicCnt] + hours[topicCnt]; hour ++) {
					for (int i = 0; i < partitionNum; i ++) {
						for (int j = 0; j < indexes_hours.get(hour - 1).get(i).size(); j ++) {
							if (ids[indexes_hours.get(hour - 1).get(i).get(j)] > topic.getQueryTweetTime()) {
								continue;
							}
							totalSize ++;
						}
					}
				}
				int selectedSize = 0;
				int finalHour = 24 * days[topicCnt] + hours[topicCnt];
				int[] partitions = determinePartition(centers_hours.get(finalHour - 2), queryVector[topicCnt], top, clusterindexes);
				for (int partition : partitions) {
					for (int hour = 1; hour <= finalHour - 1; hour ++) {
						for (int idx = 0; idx < indexes_hours.get(hour - 1).get(partition).size(); idx ++) {
							int i = indexes_hours.get(hour - 1).get(partition).get(idx);
							if (ids[i] > topic.getQueryTweetTime()) {
								continue;
							}
							selectedSize ++;
							float score = 0.0F;
							for (int t = 0; t < c; t++) {
								float prob = (float)(cf[topicCnt].getFreq(queryterms.get(t)) + 1) / (cf[topicCnt].getTotalTermCnt() + 1);
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
				for (int partition = 0; partition < partitionNum; partition ++) {
					for (int idx = 0; idx < indexes_hours.get(finalHour - 1).get(partition).size(); idx ++) {
						int i = indexes_hours.get(finalHour - 1).get(partition).get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (float)(cf[topicCnt].getFreq(queryterms.get(t)) + 1) / (cf[topicCnt].getTotalTermCnt() + 1);
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
				
				int count = 1;
				for (PairOfIntFloat pair : topN.extractAll()) {
					bw.write(String.format("%d Q0 %s %d %f kmeans", Integer.parseInt(topic.getId().substring(2)), ids[pair.getKey()], count, pair.getValue()));
					bw.newLine();
					count ++;
				}
				topicCnt ++;
				avgperctg += (float)(selectedSize) / totalSize;
			}
			System.out.println(top + "\t" + (avgperctg / topicCnt));
			bw.close();
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

	public static int[] determinePartition(double[][] centers, double[] queryVector, int top, Set<Integer> clusterindexes) {
//	public static int[] determinePartition(double[][] centers, double[] queryVector, int partition) {
		TreeMap<Double, Integer> all = new TreeMap<Double, Integer>(new ScoreComparator());
		// Euclidean distance
//		for(int i = 0; i < centers.length; i ++){
//			double distance = 0;
//			for (int j = 0; j < queryVector.length; j ++) {
//				distance += (centers[i][j] - queryVector[j]) * (centers[i][j] - queryVector[j]);
//			}
//			all.put(distance, i);
//		}
		
		// Cosine similarity
		for (int i : clusterindexes) {
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
			all.put(similarity, i);
		}
		
		int[] result = new int[all.size()];
//		int[] result = new int[partition];
		int count = 0;
		for (Entry<Double, Integer> entry : all.entrySet()) {
			if (count < top) {
				result[count] = entry.getValue();
				count ++;
			} else {
				break;
			}
		}
		return result;
	}
}
