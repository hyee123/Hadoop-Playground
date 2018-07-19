ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imbdlink:chararray);

nameLookup = FOREACH metadata GENERATE movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;

ratingsByMovie = GROUP ratings BY movieID;

ratingAndCount = FOREACH ratingsByMovie GENERATE group AS movieID, COUNT(ratings.rating) AS totalCount,
	AVG(ratings.rating) AS avgRating;

badMovies = Filter ratingAndCount BY avgRating < 2.0;

badMovies_withName = JOIN badMovies BY movieID, nameLookup BY movieID;

worstMoviesWithMostRating = ORDER badMovies_withName BY badMovies::totalCount;

finalAnswer = FOREACH worstMoviesWithMostRating GENERATE nameLookup::movieTitle,
	nameLookup::movieID, badMovies::totalCount, badMovies::avgRating;

Dump finalAnswer;
