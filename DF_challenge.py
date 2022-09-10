from json import load
from pyspark.sql import SparkSession, Row

def loadMovieNames():
    out = {}
    f = open("ml-100k/u.item")
    for row in f:
        fields = row.split("|")
        out[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return out

def parseInput(row):
    fields = row.value.split()
    return Row(userID=int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # build a sparksession which is an entry point to programming with Spark
    spark = SparkSession.builder.appName("WorstMovie10").getOrCreate()
    movieNames = loadMovieNames()
    
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    
    # read returns an DF object. .rdd turns DF -> RDD
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # rdd type to perform row wise operation
    ratingsRDD = lines.map(parseInput)
    ratings = spark.createDataFrame(ratingsRDD).cache()

    elligible_movies = ratings.groupBy("movieID").count().filter("count > 10")
    avgRatings = ratings.groupBy("movieID").avg("rating").withColumnRenamed("avg(rating)", "avgRating").filter("avgRating < 2")

    elligible_movies = elligible_movies.join(avgRatings, "movieID")

    out = elligible_movies.sort(elligible_movies["count"].desc()).take(20)
    
    
    for line in out:
        print(movieNames[line.movieID].decode('ascii', 'ignore'), line['count'], line["avgRating"])