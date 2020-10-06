
--Counts the number of movies per genre 
CREATE TABLE query1 AS 
SELECT name AS name, 
COUNT(movieid) AS moviecount 
FROM hasagenre INNER JOIN genres ON hasagenre.genreid= genres.genreid
GROUP BY genres.name;


--Average Rating of each genre
CREATE TABLE query2 AS
SELECT name, 
AVG(rating) AS rating
FROM ratings INNER JOIN hasagenre ON hasagenre.movieid =ratings.movieid
INNER JOIN genres ON hasagenre.genreid =genres.genreid
GROUP BY genres.name ; 


-- Counts the number of ratings per movie
CREATE TABLE query3 AS
SELECT title , 
COUNT(title) AS countofratings
FROM movies INNER JOIN ratings 
ON movies.movieid= ratings.movieid
GROUP BY movies.movieid
HAVING COUNT(title)>=10 
ORDER BY COUNT(title);


--Create a table with movie id & Movie title for movies in the genre 'Comedy'
CREATE TABLE query4 AS
SELECT movies.movieid,
movies.title
FROM movies INNER JOIN hasagenre
ON movies.movieid = hasagenre.movieid
INNER JOIN genres ON hasagenre.genreid =genres.genreid
WHERE genres.name='Comedy' ;


--Average Rating Per Movie 
CREATE TABLE query5 AS
SELECT title,
AVG(rating) AS average 
FROM ratings INNER JOIN movies
ON ratings.movieid = movies.movieid
GROUP BY title ;



-- AVG rating of the genre 'Comedy'
CREATE TABLE query6 AS 
SELECT AVG(rating) AS average
FROM ratings INNER JOIN hasagenre ON hasagenre.movieid =ratings.movieid
INNER JOIN genres ON hasagenre.genreid =genres.genreid
WHERE genres.name ='Comedy';



--Use a temporary table to find all the unique movieid in both comedy and romance and then 
-- find the average of romcoms as seen in query7

CREATE TABLE query7temptable AS 
SELECT DISTINCT userid,movies.movieid, rating, title
FROM movies INNER JOIN hasagenre ON movies.movieid =hasagenre.movieid
INNER JOIN genres ON hasagenre.genreid =genres.genreid INNER JOIN ratings ON hasagenre.movieid=ratings.movieid 
WHERE movies.movieid IN
(SELECT movieid FROM hasagenre INNER JOIN genres ON hasagenre.genreid =genres.genreid WHERE genres.name='Comedy')
AND movies.movieid IN
(SELECT movieid FROM hasagenre INNER JOIN genres ON hasagenre.genreid =genres.genreid WHERE genres.name='Romance');


CREATE TABLE query7 AS
SELECT AVG(rating) AS average 
FROM query7temptable ;




--Use a temporary table to find all the unique movieid in Romance And not in comedy 
-- find the average of non-comedy romance movies


CREATE TABLE query8template AS 
SELECT DISTINCT userid,movies.movieid, title, rating
FROM movies INNER JOIN hasagenre ON movies.movieid =hasagenre.movieid
INNER JOIN genres ON hasagenre.genreid =genres.genreid INNER JOIN ratings ON hasagenre.movieid=ratings.movieid 
WHERE movies.movieid IN
(SELECT movieid FROM hasagenre INNER JOIN genres ON hasagenre.genreid =genres.genreid WHERE genres.name='Romance'
EXCEPT 
SELECT movieid FROM hasagenre INNER JOIN genres ON hasagenre.genreid =genres.genreid WHERE genres.name='Comedy');

CREATE TABLE query8 AS 
SELECT AVG(rating) AS average 
FROM query8template ;




--THE PROCEEDING TABLES WILL CREATE MOVIE RECOMMENDATIONS BASED OFF OF MOVIES
-- THE USER HAS ALREADY SEEN 


CREATE TABLE l  AS
SELECT DISTINCT m.title, r.rating , m.movieid
FROM movies AS m, ratings AS r, genres , hasagenre
WHERE m.movieid= r.movieid AND m.movieid=hasagenre.movieid AND r.userid = :v1;


--movie_id and user ratings
CREATE TABLE query9 AS
SELECT movieid, rating 
FROM l AS user_rating, 
query5 as database_rating
WHERE user_rating.title=database_rating.title ;


--all movie average database raings
CREATE TABLE all_movie_ratings AS 
SELECT movies.movieid, average 
FROM movies, query5
WHERE movies.title = query5.title;


--all movie average database raings that the user hasnt rated 
CREATE TABLE users_unrated_movies AS 
SELECT DISTINCT all_movie_ratings.movieid AS unrated_movieid, 
average AS avg_unrated_movie
FROM all_movie_ratings, query9
WHERE all_movie_ratings.movieid IN
(SELECT movieid FROM all_movie_ratings 
EXCEPT 
SELECT movieid FROM query9 );



--movie averages from the database that the user has rated along with movieid and user rating

CREATE TABLE users_rated_movies_database_ratings AS
SELECT query9.movieid AS rated_movieid, 
average AS avg_rated_movie,
rating AS user_rating
FROM query9, query5, movies 
WHERE query9.movieid= movies.movieid AND movies.title= query5.title ;


--cross join users seen and unseen movies along with average rating so that 
CREATE TABLE similarity AS 
SELECT *,
1-ABS(avg_rated_movie - avg_unrated_movie)/5 AS sim
FROM users_rated_movies_database_ratings 
CROSS JOIN users_unrated_movies ;

CREATE TABLE predictions AS 
SELECT *,
user_rating*sim AS pred 
FROM similarity;


CREATE TABLE groupings AS 
SELECT unrated_movieid, sum(pred)/sum(sim) AS final_pred
FROM predictions
GROUP BY unrated_movieid ;



CREATE TABLE query10 AS
SELECT title 
FROM groupings, movies
WHERE  groupings.unrated_movieid=movies.movieid AND groupings.final_pred >3.9 ;










