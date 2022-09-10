# Extends RDD to a DataFrame object
# DataFrame:
# - Contain Row objects
# - Can run SQL queries
# - Has a schema (leading to more efficient storage)
# - Read and write to JSON, HIVE, parquet
# - Communicates with JDBC/ODBC, Tableau

from pyspark.sql import SQLContext, Row
hiveContext = HiveContext(sc)
inputData = spark.read.json(dataFile)
inputData.createOrReplaceTempView("myStructuredStuff")
myResultDataFrame = hiveContext.sql("""SELECT foo FROM bar ORDER BY foobar""")

myResultDataFrame.show()
myResultDataFrame.select("someFieldName")
myResultDataFrame.filter(myResultDataFrame("someFieldName" > 200))
myResultDataFrame.groupBy(myResultDataFrame("someFieldName")).mean()
myResultDataFrame.rdd().map(mapperFunction)

# In Spark 2.0, a DataFrame is really a DataSet of Row objects.
# DataSets can wrap known, typed data too. But this is mostly transparent to you in Python, since Python is dynamically typed.

# Shell access
# Spark SQL exposes a JDBC/ODBC server (if you built Spark with Hive support)
# Start it with sbin/start-thriftserver.sh
# Listens on port 10000 by default

# User-defined functions (UDF's)
from pyspark.sql.types import IntegerType
hiveCtx.registerFuction("square", lambda x:x*x, IntegerType())
df = hiveCtx.sql("SELECT square('someNumericFiled') FROM tableName")
# mllib etc all has dataset-based API which is a common denominator between different systems to pass data around. Easier ways to use other capabilities 