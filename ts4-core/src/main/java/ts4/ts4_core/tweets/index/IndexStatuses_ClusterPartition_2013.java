/* Build hourly/daily partitioned index on the collection based on the kmeans clustering result
 * Usage: sh target/appassembler/bin/IndexStatuses_ClusterPartition_2013 -collection [] 
 * -kmeansclusters {kmeansClustersPath} -dimension {dimension} -partition {partitionNum}
 * -index [] -optimize
 */

package ts4.ts4_core.tweets.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import cc.twittertools.corpus.data.Status;
import cc.twittertools.index.TweetAnalyzer;
import ts4.ts4_core.tweets.corpus.JsonStatusCorpusReader;

/**
 * Reference implementation for indexing statuses.
 */
public class IndexStatuses_ClusterPartition_2013 {
	private static final Logger LOG = Logger.getLogger(IndexStatuses_ClusterPartition_2013.class);
	public static final Analyzer ANALYZER = new TweetAnalyzer(Version.LUCENE_43);
	
	public static enum StatusField {
		ID("id"),
		SCREEN_NAME("screen_name"),
		EPOCH("epoch"),
		TEXT("text"),
		LANG("lang"),
		IN_REPLY_TO_STATUS_ID("in_reply_to_status_id"),
		IN_REPLY_TO_USER_ID("in_reply_to_user_id"),
		FOLLOWERS_COUNT("followers_count"),
		FRIENDS_COUNT("friends_count"),
		STATUSES_COUNT("statuses_count"),
		RETWEETED_STATUS_ID("retweeted_status_id"),
		RETWEETED_USER_ID("retweeted_user_id"),
		RETWEET_COUNT("retweet_count");

		public final String name;

		StatusField(String s) {
			name = s;
		}
	};

	private static final String COLLECTION_OPTION = "collection";
	private static final String KMEANS_CLUSTER_OPTION = "kmeansclusters";
	private static final String DIMENSION = "dimension";
	private static final String PARTITION = "partition";
	private static final String INDEX_OPTION = "index";
	private static final String OPTIMIZE_OPTION = "optimize";
	private static final String STORE_TERM_VECTORS_OPTION = "store";

	@SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(new Option(OPTIMIZE_OPTION, "merge indexes into a single segment"));
		options.addOption(new Option(STORE_TERM_VECTORS_OPTION, "store term vectors"));

		options.addOption(OptionBuilder.withArgName("dir").hasArg()
				.withDescription("source collection directory").create(COLLECTION_OPTION));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("kmeans cluster centers path").create(KMEANS_CLUSTER_OPTION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("dimension").create(DIMENSION));
		options.addOption(OptionBuilder.withArgName("arg").hasArg()
				.withDescription("partition").create(PARTITION));
		options.addOption(OptionBuilder.withArgName("dir").hasArg()
				.withDescription("index location").create(INDEX_OPTION));

		CommandLine cmdline = null;
		CommandLineParser parser = new GnuParser();
		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			System.exit(-1);
		}

		if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(KMEANS_CLUSTER_OPTION) || !cmdline.hasOption(DIMENSION) || !cmdline.hasOption(PARTITION) || !cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(IndexStatuses_ClusterPartition_2013.class.getName(), options);
			System.exit(-1);
		}

		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
		String kmeansClusterPath = cmdline.getOptionValue(KMEANS_CLUSTER_OPTION);
		int dimension = Integer.parseInt(cmdline.getOptionValue(DIMENSION));
		int partitionNum = Integer.parseInt(cmdline.getOptionValue(PARTITION));
		String indexPath = cmdline.getOptionValue(INDEX_OPTION);

		final FieldType textOptions = new FieldType();
		textOptions.setIndexed(true);
		textOptions.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
		textOptions.setStored(true);
		textOptions.setTokenized(true);        
		if (cmdline.hasOption(STORE_TERM_VECTORS_OPTION)) {
			textOptions.setStoreTermVectors(true);
		}

		LOG.info("collection: " + collectionPath);
		LOG.info("clusters: " + kmeansClusterPath);
		LOG.info("index: " + indexPath);

		long startTime = System.currentTimeMillis();
		File file = new File(collectionPath);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}
		
		File outputFile = new File(indexPath);
		if (!outputFile.exists()) {
			outputFile.mkdir();
		}
		
		Directory[] dirHourly = new Directory[partitionNum];
		IndexWriter[] writerHourly = new IndexWriter[partitionNum];
		Directory[] dirDaily = new Directory[partitionNum];
		IndexWriter[] writerDaily = new IndexWriter[partitionNum];
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses_ClusterPartition_2013.ANALYZER);
		config.setOpenMode(OpenMode.CREATE);
		
		JsonStatusCorpusReader stream = new JsonStatusCorpusReader(file);
		int cnt = 0;
		int hour = 1;
		int day = 1;
		Status status;
		String prevHour = "";
		String prevDay = "";
		Map<Integer, Integer> clusterAssignHourly = null;
		Map<Integer, Integer> clusterAssignDaily = null;
		try {
			while ((status = stream.next()) != null) {
				if (status.getText() == null) {
					continue;
				}

				Document doc = new Document();
				doc.add(new LongField(StatusField.ID.name, status.getId(), Field.Store.YES));
				doc.add(new LongField(StatusField.EPOCH.name, status.getEpoch(), Field.Store.YES));
				doc.add(new TextField(StatusField.SCREEN_NAME.name, status.getScreenname(), Store.YES));

				doc.add(new Field(StatusField.TEXT.name, status.getText(), textOptions));

				doc.add(new IntField(StatusField.FRIENDS_COUNT.name, status.getFollowersCount(), Store.YES));
				doc.add(new IntField(StatusField.FOLLOWERS_COUNT.name, status.getFriendsCount(), Store.YES));
				doc.add(new IntField(StatusField.STATUSES_COUNT.name, status.getStatusesCount(), Store.YES));

				long inReplyToStatusId = status.getInReplyToStatusId();
				if (inReplyToStatusId > 0) {
					doc.add(new LongField(StatusField.IN_REPLY_TO_STATUS_ID.name, inReplyToStatusId, Field.Store.YES));
					doc.add(new LongField(StatusField.IN_REPLY_TO_USER_ID.name, status.getInReplyToUserId(), Field.Store.YES));
				}

				String lang = status.getLang();
				if (!lang.equals("unknown")) {
					doc.add(new TextField(StatusField.LANG.name, status.getLang(), Store.YES));
				}

				long retweetStatusId = status.getRetweetedStatusId();
				if (retweetStatusId > 0) {
					doc.add(new LongField(StatusField.RETWEETED_STATUS_ID.name, retweetStatusId, Field.Store.YES));
					doc.add(new LongField(StatusField.RETWEETED_USER_ID.name, status.getRetweetedUserId(), Field.Store.YES));
					doc.add(new IntField(StatusField.RETWEET_COUNT.name, status.getRetweetCount(), Store.YES));
					if ( status.getRetweetCount() < 0 || status.getRetweetedStatusId() < 0) {
						LOG.warn("Error parsing retweet fields of " + status.getId());
					}
				}

				String curFileName = stream.getCurFileName();
				if (!curFileName.equals(prevHour)) {
					LOG.info("hour" + hour);
					// Find hourly cluster assignments and store in a map
					clusterAssignHourly = new HashMap<Integer, Integer>();
					try {
						File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-hour" + hour + "-trial1").listFiles();
						Arrays.sort(files);
						for (File f : files) {
							if (f.getName().startsWith("part")) {
								BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f.getPath())));
								String line;
								while((line = br.readLine()) != null) {
									line = line.substring(1, line.length() - 1);
									String[] indexmap = line.split(",");
									clusterAssignHourly.put(Integer.parseInt(indexmap[0]), Integer.parseInt(indexmap[1]));
								}
								br.close();
							}
						}
					} catch(Exception e){
						System.out.println("File not found");
					}
					
					if (!prevHour.equals("")) {
						for (int i = 0; i < partitionNum; i ++) {
							if (cmdline.hasOption(OPTIMIZE_OPTION)) {
								writerHourly[i].forceMerge(1);
							}
							writerHourly[i].close();
							dirHourly[i].close();
						}
					}
					for (int i = 0; i < partitionNum; i ++) {
						String indexHourlyPath = indexPath + "/hour" + hour;
						new File(indexHourlyPath).mkdir();
						dirHourly[i] = FSDirectory.open(new File(indexHourlyPath + "/part" + (i + 1)));
						writerHourly[i] = new IndexWriter(dirHourly[i], config);
					}
					hour ++;
					prevHour = curFileName;
				}
				writerHourly[clusterAssignHourly.get(cnt)].addDocument(doc);
				
				String curDay = curFileName.split("-")[2];
				if (!curDay.equals(prevDay)) {
					LOG.info("day" + day);
					// Find daily cluster assignments and store in a map
					clusterAssignDaily = new HashMap<Integer, Integer>();
					try {
						File[] files = new File(kmeansClusterPath + "/clusterassign-d" + dimension + "-day" + day + "-trial1").listFiles();
						Arrays.sort(files);
						for (File f : files) {
							if (f.getName().startsWith("part")) {
								BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f.getPath())));
								String line;
								while((line = br.readLine()) != null) {
									line = line.substring(1, line.length() - 1);
									String[] indexmap = line.split(",");
									clusterAssignDaily.put(Integer.parseInt(indexmap[0]), Integer.parseInt(indexmap[1]));
								}
								br.close();
							}
						}
					} catch(Exception e){
						System.out.println("File not found");
					}
					
					if (!prevDay.equals("")) {
						for (int i = 0; i < partitionNum; i ++) {
							if (cmdline.hasOption(OPTIMIZE_OPTION)) {
								writerDaily[i].forceMerge(1);
							}
							writerDaily[i].close();
							dirDaily[i].close();
						}
					}
					for (int i = 0; i < partitionNum; i ++) {
						String indexDailyPath = indexPath + "/day" + day;
						new File(indexDailyPath).mkdir();
						dirDaily[i] = FSDirectory.open(new File(indexDailyPath + "/part" + (i + 1)));
						writerDaily[i] = new IndexWriter(dirDaily[i], config);
					}
					day ++;
					prevDay = curDay;
				}
				writerDaily[clusterAssignDaily.get(cnt)].addDocument(doc);
				
				cnt ++;
				if (cnt % 100000 == 0) {
					LOG.info(cnt + " statuses indexed");
				}
			}

			LOG.info(String.format("Total of %s statuses added", cnt));
			LOG.info("Total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			for (int i = 0; i < partitionNum; i ++) {
				if (cmdline.hasOption(OPTIMIZE_OPTION)) {
					writerHourly[i].forceMerge(1);
					writerDaily[i].forceMerge(1);
				}
				writerHourly[i].close();
				dirHourly[i].close();
				writerDaily[i].close();
				dirDaily[i].close();
			}
			stream.close();
		}
	}
}