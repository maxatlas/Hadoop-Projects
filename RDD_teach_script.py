from pyspark import SparkConf, SparkContext

nums = parallelize([1,2,3,4])
sc.textFile("file://...") # or s3n://, hdfs://
hiveCtx = HiveContxt(sc) # create an RDD from HIVE and do SQL query on the HIVE context from within Spark.
rows = hiveCtx.sql("SELECT name, age FROM users;") 
 
# Can also create RDD from 
#  - JDBC
#  - Cassandra
#  - HBase
#  - ElasticSearch
#  - JSON, CSV, sequence files, object files, various compressed formats

# RDD functions for transformation:
# - map
# - flatmap
# - filter
# - distinct
# - sample
# - union, intersection, subtract, cartesian

rdd = sc.parallelize([1,2,3,4])
sqauredRDD = rdd.map(lambda x:x * x)

# RDD actions:
# - collect: take all of the RDD's results and suck them down to the driver script.
# - count: how many rows are in the RDD
# - countByValue: count unique values
# - take: only take the top n results could be for debugging
# - top: top n results could be for debugging
# - reduce: 
# - etc.

# Nothing happens in driver program until an action is called.
