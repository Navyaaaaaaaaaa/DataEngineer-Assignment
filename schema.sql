PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS movies (
    movieId INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    year INTEGER,
    imdb_id TEXT UNIQUE,
    director TEXT,
    plot TEXT,
    box_office TEXT,
    runtime INTEGER,
    language TEXT,
    country TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INTEGER NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (movieId, genre_id),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    userId INTEGER NOT NULL,
    movieId INTEGER NOT NULL,
    rating REAL NOT NULL,
    timestamp INTEGER,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ratings_movie ON ratings(movieId);
CREATE INDEX IF NOT EXISTS idx_movies_year ON movies(year);
