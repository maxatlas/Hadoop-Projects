ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|') 
    AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, 
    ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID, AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) AS ratingCount;
lowRateMovies = FILTER avgRatings BY avgRating < 2.0;

lowRateWithData = JOIN lowRateMovies BY movieID, nameLookup BY movieID;
lowRateWithData = FOREACH lowRateWithData GENERATE nameLookup::movieTitle AS movieName, lowRateMovies::avgRating AS avgRating, lowRateMovies::ratingCount AS ratingCount;
finalOutput = ORDER lowRateWithData BY ratingCount DESC;
DUMP finalOutput;