from ast import If
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS # recommendation algorithm Netflix
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    f = open("ml-100k/u.item")
    for line in f:
        fields = line.split('|')
        movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()
    # This line is necessary on HDP 2.6.5:
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()
    counts = ratings.groupBy("movieID").count()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")

    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # .withColumn -> create new column
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))
    recommendations = model.transform(popularMovies)  # output is still a DF object: Row(movieID, userID, prediction)

    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20) # sort by value

    for recommendation in topRecommendations:
        print(movieNames[recommendation['movieID']], recommendation['prediction'])


    spark.stop()