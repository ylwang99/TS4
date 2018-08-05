/* Build hourly/daily index on the collection based on the kmeans clustering result
 * Usage: sh target/appassembler/bin/IndexStatuses_Partition_2011 -collection [] 
 * -index [] -optimize
 */

package ts4.ts4_core.tweets.index;

import java.io.File;
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
public class IndexStatuses_Partition_2011 {
	private static final Logger LOG = Logger.getLogger(IndexStatuses_Partition_2011.class);
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

		if (!cmdline.hasOption(COLLECTION_OPTION) || !cmdline.hasOption(INDEX_OPTION)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(IndexStatuses_Partition_2011.class.getName(), options);
			System.exit(-1);
		}

		String collectionPath = cmdline.getOptionValue(COLLECTION_OPTION);
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
		LOG.info("index: " + indexPath);

		long startTime = System.currentTimeMillis();
		File file = new File(collectionPath);
		if (!file.exists()) {
			System.err.println("Error: " + file + " does not exist!");
			System.exit(-1);
		}
		
		for (int hour = 0; hour < 408; hour ++) {
			String indexHourlyPath = indexPath + "/hour" + hour;
			new File(indexHourlyPath).mkdir();
		}
		for (int day = 0; day < 17; day ++) {
			String indexDailyPath = indexPath + "/day" + day;
			new File(indexDailyPath).mkdir();
		}
				
		File outputFile = new File(indexPath);
		if (!outputFile.exists()) {
			outputFile.mkdir();
		}
		
		Directory[] dirHourly = new Directory[408];
		IndexWriter[] writerHourly = new IndexWriter[408];
		Directory[] dirDaily = new Directory[17];
		IndexWriter[] writerDaily = new IndexWriter[17];
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, IndexStatuses_Partition_2011.ANALYZER);
		config.setOpenMode(OpenMode.CREATE);
		
		String[] days = {"Jan23", "Jan24", "Jan25", "Jan26", "Jan27", "Jan28", "Jan29", "Jan30", "Jan31", "Feb01", "Feb02", "Feb03", "Feb04", "Feb05", "Feb06", "Feb07", "Feb08"};
		Map<String, Integer> dayMap = new HashMap<String, Integer>();
		for (int i = 0; i < days.length; i ++) {
			dayMap.put(days[i], i);
		}
		
		JsonStatusCorpusReader stream = new JsonStatusCorpusReader(file);
		int cnt = 0;
		int hour = 1;
		int day = 1;
		Status status;
		int prevHour = 0;
		int prevDay = 0;
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

				String createdAt = status.getCreatedAt();
				String createdHour = createdAt.split(":")[0];
				String[] createdHourArr = createdHour.split(" ");
				hour = dayMap.get(createdHourArr[1] + createdHourArr[2]) * 24 + Integer.parseInt(createdHourArr[3]) + 1;
				if (hour != prevHour) {
					LOG.info("hour" + hour);
					
					if (prevHour != 0) {
						if (cmdline.hasOption(OPTIMIZE_OPTION)) {
							writerHourly[prevHour - 1].forceMerge(1);
						}
						writerHourly[prevHour - 1].close();
						dirHourly[prevHour - 1].close();
					}
					
					dirHourly[hour - 1] = FSDirectory.open(new File(indexPath + "/hour" + hour));
					writerHourly[hour - 1] = new IndexWriter(dirHourly[hour - 1], config);
					prevHour = hour;
				}
				writerHourly[hour - 1].addDocument(doc);
				
				day = dayMap.get(createdHourArr[1] + createdHourArr[2]) + 1;
				if (day != prevDay) {
					LOG.info("day" + day);
					
					if (prevDay != 0) {
						if (cmdline.hasOption(OPTIMIZE_OPTION)) {
							writerDaily[prevDay - 1].forceMerge(1);
						}
						writerDaily[prevDay - 1].close();
						dirDaily[prevDay - 1].close();
					}
					
					dirDaily[day - 1] = FSDirectory.open(new File(indexPath + "/day" + day));
					writerDaily[day - 1] = new IndexWriter(dirDaily[day - 1], config);
					prevDay = day;
				}
				writerDaily[day - 1].addDocument(doc);
				
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
			if (cmdline.hasOption(OPTIMIZE_OPTION)) {
				writerHourly[hour - 1].forceMerge(1);
				writerDaily[hour - 1].forceMerge(1);
			}
			writerHourly[hour - 1].close();
			dirHourly[hour - 1].close();
			writerDaily[day - 1].close();
			dirDaily[day - 1].close();
			stream.close();
		}
	}
}