-- 1. Highest average rating movie
SELECT m.title, ROUND(AVG(r.rating),2) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.title
ORDER BY avg_rating DESC
LIMIT 1;

-- 2. Top 5 genres by average rating
SELECT g.genre, ROUND(AVG(r.rating),2) AS avg_rating
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movieId = r.movieId
GROUP BY g.genre
ORDER BY avg_rating DESC
LIMIT 5;

-- 3. Director with most movies
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. Average rating by year
SELECT year, ROUND(AVG(r.rating),2) AS avg_rating
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY year
ORDER BY year;
