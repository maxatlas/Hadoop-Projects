SELECT n.title, avgRating
FROM
(SELECT movieID, AVG(rating) as avgRating, count(movieID) as ratingCount
FROM ratings
GROUP BY movieID
ORDER BY avgRating DESC) t JOIN names n ON t.movieID = n.movieID
WHERE ratingCount > 10;