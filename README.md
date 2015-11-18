Tweet Streaming Selective Search with Spark
=============
This is a tool to perform tweet streaming selective search with Spark provided Maching Learning library [MLlib](http://spark.apache.org/docs/latest/mllib-clustering.html). For getting document vectors, document collections are first trained with [GloVe](http://nlp.stanford.edu/projects/glove/) to get vectors for each unique word and then each document is representated as a vector that's the average of each individual word's vectors.

Getting Started
--------------
1. You can clone the repo with the following command:

	```
	$ git clone git://github.com/ylwang/TS4.git
	``` 
2. Once you've cloned the repository, change directory into `ts4-core` and build the package with Maven:

	```
	$ cd TS4/ts4-core
	$ mvn clean package appassembler:assemble
	```
3. Generate input data for GloVe:

	```
	$ sh target/appassembler/bin/GenInput4Glove -input {textFile} -output {textInOneLineFile}
	```
	Here the textFile is in the format of:
	```
	docid1 token1 token2 ...
	docid2 token1 token2 ...
	...
	```
	seperated by white space.

4. GloVe make:

	```
	$ cd ../glove/
	$ make
	```
5. Run GloVe:
	```
	$ ./run.sh {textInOneLineFile} {vectorsPath}
	```
	Here the vectorsPath stores all the vectors information for the collection, and vectorsPath/vectors.txt has the word vectors in the format of:
	```
	word1 ele1 ele2 ...
	word2 ele1 ele2 ...
	...
	```
	By default, we set the window size to be 15 and word vector is in 50 dimensions. To have more variants, feel free to modify glove/run.sh accordingly.

6. To store the vectors into a map:
	```
	$ cd ../ts4-core
	$ sh target/appassembler/bin/VectorToMap -vectors {vectorsPath/vectors.txt} -output {vectorsMapFile}
	```
7. Convert document collections into vectors:

	```
	$ sh target/appassembler/bin/DocToVec -input {textFile} -vectors {vectorsMapFile} -output {docvectorsFile}
	```
8. Put docvectorsFile onto HDFS:

	```
	$ hadoop fs -put {docvectorsFile}
	```
9. Run k-means (here we set K to be 100 and number of iterations to be 20) on Spark:
	```
	spark-shell --master yarn-client --num-executors 40 --driver-memory 256G --executor-memory 50G --conf spark.storage.memoryFraction=1
	
	import org.apache.spark.mllib.clustering.KMeans
	import org.apache.spark.mllib.linalg.Vectors
	
	val tweets = sc.textFile("{docvectorsFile}")
	val parsedData = tweets.map(s => {
	 val arr = s.split(" ", 2);
	 new Tuple2(arr(0), 
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
10. Put clustercenters and clusterassignment files on local disk:

	```
	$ hadoop fs -get {clustercentersFile}
	$ hadoop fs -get {clusterassignFile}
	```
	clustercentersFile stores the centroids of each cluster, and clusterassignFile stores the cluster assignment for each docid in the format of:
	```
	(docid1, clusterid)
	(docid2, clusterid)
	...
	```
	and cluster id starts from 0.
