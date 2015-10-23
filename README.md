Tweet Streaming Selective Search with Spark
=============
This is a tool to perform tweet streaming selective search with Spark provided Maching Learning library [MLlib](http://spark.apache.org/docs/latest/mllib-clustering.html). For getting document vectors, document collections are first trained with [GloVe](http://nlp.stanford.edu/projects/glove/) to get vectors for each unique word and then each document is representated as a vector that's the average of each individual word's vectors.

This provides an API with takes document collection as input in the format of:
```
docid1 token1 token2 ...
docid2 token1 token2 ...
...
```
and outputs cluster assignments for the documents in the format of:
```
(docid1, clusterid)
(docid2, clusterid)
...
```

In the following, we describe how to run TS4 with tweets data and perform evaluation on it in the retrieval task.
Getting Started
--------------
1. You can clone the repo with the following command:

	```
	$ git clone git://github.com/ylwang/TS4.git
	``` 
2. Once you've cloned the repository, change directory into `ts4-core`, switch to branch ts4-with-tweets and build the package with Maven:

	```
	$ cd TS4
	$ git checkout ts4-with-tweets
	$ cd ts4-core
	$ mvn clean package appassembler:assemble
	```
	
Preparing Tweets Data
--------------
1. Generate tweet text in the format of (docindex token1 token2 ...):

	```
	$ sh target/appassembler/bin/GenerateTweetText -collection {collectionPath} -output {tweetTextPath} (-hourly)
	```
Here the -hourly option enables us to store the tweets data in different files on an hourly-basis.

Running API
--------------
1. Generate input file to be fed into glove, basically, make the collection a one line document delimited by white space:

	```
	$ sh target/appassembler/bin/GenInput4Glove -input {tweetTextPath} -output {onelineFile}
	```
2. GloVe make:

	```
	$ cd ../glove/
	$ make
	```
3. Run GloVe:
	```
	$ ./run.sh {onelineFile} {vectorsPath}
	```
	Here the vectorsPath stores all the vectors information for the collection, and vectorsPath/vectors.txt has the word vectors in the format of:
	```
	word1 ele1 ele2 ...
	word2 ele1 ele2 ...
	...
	```
	By default, we set the window size to be 15 and word vector is in 50 dimensions. To have more variants, feel free to modify glove/run.sh accordingly.

4. To store the vectors into a map so it's easier to generate vector representations for docs:
	```
	$ cd ../ts4-core
	$ sh target/appassembler/bin/VectorToMap -vectors {vectorsPath/vectors.txt} -output {vectorsMapFile}
	```
5. Convert document collections into vectors:

	```
	$ sh target/appassembler/bin/DocToVec -input {tweetTextPath} -vectors {vectorsMapFile} -output {docvectorsFile}
	```
6. Put docvectorsFile onto HDFS:

	```
	$ hadoop fs -put {docvectorsFile}
	```
7. Run k-means (here we set K to be 100 and number of iterations to be 20) on Spark:
	```
	spark-shell --master yarn-client --num-executors 40 --driver-memory 256G --executor-memory 50G --conf spark.storage.memoryFraction=1
	
	import org.apache.spark.mllib.clustering.KMeans
	import org.apache.spark.mllib.linalg.Vectors
	
	val tweets = sc.textFile("{docvectorsFile}")
	val parsedData = tweets.map(s => {
	 val arr = s.split(" ", 2);
	 new Tuple2(arr(0).toLong, 
	   Vectors.dense(arr(1).split(" ").map(_.toDouble)))
	})
	val numClusters = 100
	val numIterations = 20
	val clusters = KMeans.train(parsedData.map(_._2), numClusters, numIterations)
	
	val clusterCenters = clusters.clusterCenters
	val clusterCentersRDD = sc.parallelize(clusterCenters)
	clusterCentersRDD.saveAsTextFile("{clustercentersFile}")
	
	val tweetsAssigned = parsedData.map(s => new Tuple2(s._1, clusters.predict(s._2)))
	tweetsAssigned.saveAsTextFile("{clusterassignFile}")
	```
	clustercentersFile stores the centroids of each cluster, and clusterassignFile stores the cluster assignment for each docid in the format of:
	```
	(docid1, clusterid)
	(docid2, clusterid)
	...
	```
and cluster id starts from 0.
8. Put clustercenters and clusterassignment files on local disk:

	```
	$ hadoop fs -get {clustercentersFile}
	$ hadoop fs -get {clusterassignFile}
	```

Running Queries
--------------
1. Generate query text from TREC Microblog topics, parsed by TweetAnalyzer:

	```
	$ sh target/appassembler/bin/GenerateQueryText -queries {queryPath} -output {queryTextPath}
	```
2. Generate vector representations for queries based on word vectors:

	```
	$ sh target/appassembler/bin/DocToVec -input {queryTextPath} -vectors {vectorsMap} -output {queryVecPath}
	```
3. Build index on the tweets collection:

	```
	$ sh target/appassembler/bin/IndexStatuses -collection {collectionPath} -index {indexPath} -optimize
	```
4. Generate statistics used for running queries:

	```
	$ sh target/appassembler/bin/GenerateStatistics -index {indexPath} -collection {collectionPath} -output {statisticsPath}
	```	
5. Run queries on kmeans results:

	```
	$ sh target/appassembler/bin/RunQueries -index {indexPath} -stats {statsPath} -clustercenters {clustercentersFile} \
	-clusterindexes {clusterassignFile} -dimension {dimension} -partition {partitionNum} -top {N} \
	-queries {queriesPath} -queriesvector {queryVectorPath} > {results}
	```

Evaluating Results
--------------
1. Make trec_eval:

	```
	$ cd ../etc/trec_eval.9.0/
	$ make
	```
2. Run evaluation

	```
	$ cd ../../ts4-core/
	$ ../etc/trec_eval.9.0/trec_eval -q -c {qrelsFile} {results}
	```
