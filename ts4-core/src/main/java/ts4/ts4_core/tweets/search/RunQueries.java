/* Run queries on kmeans results
 * Run: sh target/appassembler/bin/RunQueries -index {indexPath} -stats {statsPath} -clustercenters {centerPath}
 * 		-clusterindexes {clusterassignPath} -dimension {dimension} -partition {partitionNum} -top {N}
 * 		-queries {queriesPath} -queriesvector {queryVectorPath} -output {outputPath}
 */
package ts4.ts4_core.tweets.search;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
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

public class RunQueries {
	private static final Logger LOG = Logger.getLogger(RunQueries.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";
	private static final String STATS_OPTION = "stats";
	private static final String CLUSTER_CENTER_OPTION = "clustercenters";
	private static final String CLUSTER_INDEX_OPTION = "clusterindexes";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
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
				.withDescription("cluster centers").create(CLUSTER_CENTER_OPTION));
		options.addOption(OptionBuilder.withArgName("file").hasArg()
				.withDescription("cluster indexes").create(CLUSTER_INDEX_OPTION));
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

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(STATS_OPTION) || !cmdline.hasOption(CLUSTER_CENTER_OPTION) || !cmdline.hasOption(CLUSTER_INDEX_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(QUERIES_VECTOR_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueries.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		String statsPath = cmdline.getOptionValue(STATS_OPTION);
		String clusterCentersPath = cmdline.getOptionValue(CLUSTER_CENTER_OPTION);
		String clusterIndexesPath = cmdline.getOptionValue(CLUSTER_INDEX_OPTION);
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
		
		// Read in cluster centers
		LOG.info("Reading cluster centers from file.");
		double centers[][] = new double[partitionNum][dimension];
		int ind = 0;
		try {
			File[] files = new File(clusterCentersPath).listFiles();
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
        					centers[ind][j ++] = Double.parseDouble(coor);
        				}
        				ind ++;
        			}
                    br.close();
				}
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
		LOG.info("Finished reading cluster centers from file.");
			
		// Read in cluster indexes
		LOG.info("Reading cluster indexes from file.");
		List<List<Integer>> indexes = new ArrayList<List<Integer>>();
		for (int i = 0; i < partitionNum; i ++) {
			indexes.add(new ArrayList<Integer>());
		}
		try {
			File[] files = new File(clusterIndexesPath).listFiles();
			Arrays.sort(files);
			for (File file : files) {
				if (file.getName().startsWith("part")) {
					BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getPath())));
                    String line;
                    while((line = br.readLine()) != null) {
                    	line = line.substring(1, line.length() - 1);
                    	String[] indexmap = line.split(",");
                    	indexes.get(Integer.parseInt(indexmap[1])).add(Integer.parseInt(indexmap[0]));
        			}
                    br.close();
				}
			}
		} catch(Exception e){
            System.out.println("File not found");
		}
		LOG.info("Finished reading cluster indexes from file.");
		
		LOG.info("Running queries.");
//		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for ( @SuppressWarnings("unused") TrecTopic topic : topics ) {
			topicTotal ++;
		}
		double[][] queryVector = new double[topicTotal][dimension];
		ind = 0;
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

		System.out.println("top n\tavg scan size");
		for (top = 1; top <= partitionNum; top ++) {
			float avgperctg = 0.0f;
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath + "/glove_d" + dimension + "_mean_all_top" + top + ".txt")));
			int topicCnt = 0;
			for ( TrecTopic topic : topics ) {  
				List<String> queryterms = parse(ANALYZER, topic.getQuery());
				TopNScoredInts topN = new TopNScoredInts(numResults);
				int[] qids = new int[queryterms.size()];
				float[] freqs = new float[queryterms.size()];
				int c = 0;
				for (String term : queryterms) {
					qids[c] = termStats.getId(term);
					freqs[c] = termStats.getFreq(termStats.getId(term));
					c++;
				}
				
				int totalSize = 0;
				for (int i = 0; i < partitionNum; i ++) {
					for (int j = 0; j < indexes.get(i).size(); j ++) {
						if (ids[indexes.get(i).get(j)] > topic.getQueryTweetTime()) {
							continue;
						}
						totalSize ++;
					}
				}
				int[] partitions = determinePartition(centers, queryVector[topicCnt], top);
				int selectedSize = 0;
				for (int partition : partitions) {
					for (int idx = 0; idx < indexes.get(partition).size(); idx ++) {
						int i = indexes.get(partition).get(idx);
						if (ids[i] > topic.getQueryTweetTime()) {
							continue;
						}
						selectedSize ++;
						float score = 0.0F;
						for (int t = 0; t < c; t++) {
							float prob = (freqs[t] + 1) / (GenerateStatistics.TOTAL_TERMS + 1);
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

	public static int[] determinePartition(double[][] centers, double[] queryVector, int top) {
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
			all.put(similarity, i);
		}
		
		int[] result = new int[top];
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
