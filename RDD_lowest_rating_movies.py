from pyspark import SparkConf, SparkContext

"""
    All RDD object's actions included here are row level operation, and each row is of type tuple.
    Additional field of value 1 for reducedByKey operation to calculate count.
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
    return (int(fields[1]), (float(fields[2]), 1.0))

if __name__ == "__main__":
    # The main script - create our SparkContext
    # SparkConf - the configuration class. set which cluster to run from ...
    conf = SparkConf().setAppName("WorstMovies")
    sc = SparkContext(conf = conf)

    # Load up our movie ID -> movie name lookup table
    movieNames = loadMovieNames()

    # Load up the raw u.data file. The loaded file could potentially spread out across the entire cluster.
    # Return as an RDD of strings
    lines = sc.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    # Convert to (movieID, (rating, 1.0))
    movieRatings = lines.map(parseInput)

    # Reduce to (movieID, (sumOfRatings, totalRatings))
    ratingTotalsAndCount = movieRatings.reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))

    # Map to (movieID, averageRating)
    averageRatings = ratingTotalsAndCount.mapValues(lambda totalAndCount: totalAndCount[0] / totalAndCount[1])

    # Sort by average rating
    sortedMovies = averageRatings.sortBy(lambda x: x[1])

    # Take the top 10 results
    results = sortedMovies.take(10)

    for result in results:
        print(movieNames[result[0]], result[1])

        