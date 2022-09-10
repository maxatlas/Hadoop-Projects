from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

"""
    Operations look like pandas. Easier data manipulation.

"""

def loadMovieNames():
    movieNames = {}  # movieID:movieName
    f = open("ml-100k/u.item")
    for line in f:
        fields = line.split('|')
        movieNames[int(fields[0])] = fields[1]
    f.close()
    return movieNames

# Take each line of u.data and convert it to (movieID, (rating, 1.0)). This way we can then add up all the ratings for each movie, and the total number of ratings for each movie (which lets us compute the average).
def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Create s SparkSession. How Spark 2.0 works.
    # getOrCreate(), recover from saved snapshot session if unsuccessful
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data. The session contains a Context. Returns an RDD of strings
    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")
    
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)
    
    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")
    
    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()
    
    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    spark.stop()