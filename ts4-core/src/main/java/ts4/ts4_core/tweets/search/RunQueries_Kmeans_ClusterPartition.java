/* Run queries on kmeans results day1/hour1 to record the latency
 * Run: sh target/appassembler/bin/RunQueries_Kmeans_ClusterPartition -index {indexPath} 
 *   	-kmeansclusters {kmeansclustersPath} [-hourly true] -dimension {dimension} -partition {partitionNum} 
 * 		[-top {N}] -queries {queriesPath} -queriesvector {queryVectorPath} -trial {trial}
 */
package ts4.ts4_core.tweets.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import cc.twittertools.index.TweetAnalyzer;
import cc.twittertools.index.IndexStatuses.StatusField;
import cc.twittertools.search.TrecTopic;
import cc.twittertools.search.TrecTopicSet;
import edu.umd.cloud9.io.pair.PairOfLongFloat;

public class RunQueries_Kmeans_ClusterPartition {
	private static final Logger LOG = Logger.getLogger(RunQueries_Kmeans_ClusterPartition.class);
	private static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);

	private static final String INDEX_OPTION = "index";		// add partition index
	private static final String KMEANS_CLUSTER_OPTION = "kmeansclusters";
	private static final String HOURS_OPTION = "hourly";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String TOP = "top";
	private static final String QUERIES_OPTION = "queries";
	private static final String QUERIES_VECTOR_OPTION = "queriesvector";
	private static final String TRIAL = "trial";

	@SuppressWarnings({ "static-access"})
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("index location").create(INDEX_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("kmeans cluster centers path").create(KMEANS_CLUSTER_OPTION));
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

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(KMEANS_CLUSTER_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(QUERIES_OPTION) || !cmdline.hasOption(QUERIES_VECTOR_OPTION) || !cmdline.hasOption(TRIAL)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(RunQueries_Kmeans_ClusterPartition.class.getName(), options);
			System.exit(-1);
		}

		String indexPath = cmdline.getOptionValue(INDEX_OPTION);
		String kmeansClusterPath = cmdline.getOptionValue(KMEANS_CLUSTER_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		int top = cmdline.hasOption(TOP) ? Integer.parseInt(cmdline.getOptionValue(TOP)) : partitionNum;
		String queryPath = cmdline.getOptionValue(QUERIES_OPTION);
		String queryVectorPath = cmdline.getOptionValue(QUERIES_VECTOR_OPTION);
		String trial = cmdline.getOptionValue(TRIAL);

		// Read in index
		File indexLocation = new File(indexPath);
		if (!indexLocation.exists()) {
			System.err.println("Error: " + indexLocation + " does not exist!");
			System.exit(-1);
		}

		if (cmdline.hasOption(HOURS_OPTION)) {
			search("hour", indexPath, kmeansClusterPath, dimension, partitionNum, top, queryPath, queryVectorPath, trial);
		} else {
			search("day", indexPath, kmeansClusterPath, dimension, partitionNum, top, queryPath, queryVectorPath, trial);
		}
	}
	
	public static void search(String hourDayOption, String indexPath, String kmeansClusterPath, int dimension, int partitionNum, int top, String queryPath, String queryVectorPath, String trial) throws Exception {
		int numResults = 1000;
		
		// Read in cluster centers
		LOG.info("Reading cluster centers from file");
		double[][] centers = new double[partitionNum][dimension];
		int ind = 0;
		try {
			File[] files = new File(kmeansClusterPath + "/clustercenters-d" + dimension + "-" + hourDayOption + "1-trial" + trial).listFiles();
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
		LOG.info("Finished reading cluster centers from file");
		
		// Read in indexes
		Similarity lgmodel = new LMDirichletSimilarity(2500.0f);
		IndexReader[] readers = new IndexReader[partitionNum];
		IndexSearcher[] searchers = new IndexSearcher[partitionNum];
		for (int i = 0; i < partitionNum; i ++) {
			readers[i] = DirectoryReader.open(FSDirectory.open(new File(indexPath + "/" + hourDayOption + "1/part" + (i + 1))));
			searchers[i] = new IndexSearcher(readers[i]);
			searchers[i].setSimilarity(lgmodel);
		}
		
		// Read in topics
		QueryParser p = new QueryParser(Version.LUCENE_43, StatusField.TEXT.name, ANALYZER);
		TrecTopicSet topics = TrecTopicSet.fromFile(new File(queryPath));
		int topicTotal = 0;
		for (@SuppressWarnings("unused") TrecTopic topic : topics) {
			topicTotal ++;
		}
		
		// Read in query vectors
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
		
		// Run queries
		LOG.info("Running queries");
		for (int topIdx = 1; topIdx <= top; topIdx ++) {
			System.out.println("top " + topIdx + " cluster");
			int N = 6;
			while (N-- > 0) {
				int topicCnt = 0;
				TopNFast[] topN = new TopNFast[topIdx];
				double total = 0;
				for (TrecTopic topic : topics) {
					for (int i = 0; i < topIdx; i ++) {
						topN[i] = new TopNFast(numResults);
					}
					Query query = p.parse(topic.getQuery());
					Filter filter = NumericRangeFilter.newLongRange(StatusField.ID.name, 0L, topic.getQueryTweetTime(), true, true);				

					double startTime = System.currentTimeMillis();
					int[] partition = determinePartition(centers, queryVector[topicCnt], topIdx);
					for (int i = 0; i < topIdx; i ++) {
						TopDocs rs = searchers[partition[i]].search(query, filter, numResults);
						for (ScoreDoc scoreDoc : rs.scoreDocs) {
							Document hit = searchers[partition[i]].doc(scoreDoc.doc);
							topN[i].add(hit.getField(StatusField.ID.name).numericValue().longValue(), scoreDoc.score);
						}
					}
					
					TopNFast topN_merged = new TopNFast(numResults);
					for (int i = 0; i < topIdx; i ++) {
						PairOfLongFloat[] scores = topN[i].extractAll();
						for (int j = 0; j < scores.length; j ++) {
							topN_merged.add(scores[j].getLeftElement(), scores[j].getRightElement());
						}
					}
					int i = 1;
					for (PairOfLongFloat pair : topN_merged.extractAll()) {
					 	// System.out.println(String.format("%s Q0 %s %d %f %s", topic.getId(), pair.getLeftElement(), i, pair.getRightElement(), runtag));
					 	i ++;
					}
					total += System.currentTimeMillis() - startTime;
					topicCnt ++;
				}
				System.out.println("Time = " + total / topicTotal + " ms");
			}
		}
		for (int i = 0; i < partitionNum; i ++) {
			readers[i].close();
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
