from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movies,
                   reducer=self.reducer_count_movies),
            MRStep(reducer=self.reducer_sort_movies)
        ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movies(self, movie, count):
        yield str(sum(count)).zfill(5), movie
    
    def reducer_sort_movies(self, counts, movies):
        for movie in movies:
            yield movie, counts

if __name__ == '__main__':
    RatingsBreakdown.run()