import os
import time
import requests
import pandas as pd
import re
import json
from rapidfuzz import fuzz
from unidecode import unidecode
from sqlalchemy import create_engine, text




# Database connection (SQLite by default)
DATABASE_URL = "sqlite:///movies.db"
OMDB_KEY = os.getenv("OMDB_API_KEY")  # You must set this in your terminal
OMDB_URL = "http://www.omdbapi.com/"

#print(" OMDB_KEY =", OMDB_KEY)


# Function to safely extract year from title
def parse_title_year(title):
    if title.endswith(")"):
        try:
            idx = title.rfind("(")
            return title[:idx].strip(), int(title[idx + 1:-1])
        except:
            return title, None
    return title, None



# simple file cache to avoid re-querying OMDb while tuning
OMDB_CACHE_PATH = "omdb_cache.json"
try:
    with open(OMDB_CACHE_PATH, "r", encoding="utf-8") as _f:
        OMDB_CACHE = json.load(_f)
except FileNotFoundError:
    OMDB_CACHE = {}

def save_omdb_cache():
    with open(OMDB_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(OMDB_CACHE, f, indent=2, ensure_ascii=False)



# Function to fetch details from OMDb API
def clean_title(title: str) -> str:
    if not title or pd.isna(title):
        return ""
    t = str(title).strip()
    # remove trailing year like " (1995)"
    t = re.sub(r"\(\s*\d{4}\s*\)\s*$", "", t)
    # remove other parentheticals (foreign alt titles) entirely
    t = re.sub(r"\(.*?\)", "", t)
    # normalize accented characters to ASCII (e.g., Cité -> Cite)
    t = unidecode(t)
    # move trailing ", The/An/A" to front -> "The Matrix"
    m = re.match(r"^(.*),\s*(the|an|a)$", t, flags=re.I)
    if m:
        t = f"{m.group(2)} {m.group(1)}"
    # replace ampersand with 'and'
    t = t.replace("&", " and ")
    # remove punctuation except colon and alphanums
    t = re.sub(r"[^0-9A-Za-z\s:]", "", t)
    # collapse spaces and trim
    t = re.sub(r"\s+", " ", t).strip()
    return t



def fetch_omdb_details(title, year=None, fuzzy_threshold=85, sleep_between_calls=0.25):
    """
    Simplified OMDb lookup using rapidfuzz.
    - Checks cache
    - Tries t=title (+y)
    - Tries t=cleaned_title
    - Falls back to s=search and fuzzy-match candidates, then fetch by imdbID
    - Prints result and caches
    """
    if not OMDB_KEY:
        print("  OMDB_API_KEY not found. Skipping enrichment.")
        return {}

    
    # internal function to call OMDb with retries
    def call_omdb(params, tries=2):
        params["apikey"] = OMDB_KEY
        for _ in range(tries):
            try:
                r = requests.get(OMDB_URL, params=params, timeout=8)
                return r.json()
            except Exception as e:
                print(f" OMDb request error for {params}: {e}")
                time.sleep(0.5)
        return None

    # cache
    cache_key = f"{title}||{year}"
    if cache_key in OMDB_CACHE:
        cached = OMDB_CACHE[cache_key] or {}
        if cached and cached.get("Response") == "True":
            print(f"🎬 Movie found: {cached.get('Title')}")

        else:
            print(f" From cache: No data for {title}")
        return cached

    # 1) exact t=title (+year)
    clean_t = clean_title(title)
    params = {"t": clean_t}
    if year:
        params["y"] = str(int(float(year)))
    print(f" Searching OMDb for: '{clean_t}' ({year})")
    data = call_omdb(params)
    time.sleep(sleep_between_calls)
    if data and data.get("Response") == "True":
        OMDB_CACHE[cache_key] = data
        save_omdb_cache()
        print(f" Movie found: {data.get('Title')}")
        return data

    # 2) cleaned title
    clean_t = clean_title(title)
    if clean_t and clean_t != title:
        params = {"t": clean_t}
        data = call_omdb(params)
        time.sleep(sleep_between_calls)
        if data and data.get("Response") == "True":
            OMDB_CACHE[cache_key] = data
            save_omdb_cache()
            print(f" Movie found: {data.get('Title')}")
            return data

    # 3) search + fuzzy-match candidates
    params = {"s": clean_t or title, "type": "movie"}
    search = call_omdb(params)
    time.sleep(sleep_between_calls)
    best = None
    best_score = 0
    if search and search.get("Response") == "True":
        src = clean_title(title).lower()
        for cand in search.get("Search", []):
            cand_norm = clean_title(cand.get("Title", "")).lower()
            score = fuzz.token_sort_ratio(src, cand_norm)
            # boost if year matches
            if score > best_score:
                best_score, best = score, cand
                

    if best and best_score >= fuzzy_threshold:
        full = call_omdb({"i": best.get("imdbID")})
        time.sleep(sleep_between_calls)
        if full and full.get("Response") == "True":
            OMDB_CACHE[cache_key] = full
            save_omdb_cache()
            print(f" Movie found (fuzzy): {best.get('Title')}")
            return full

    # nothing found
    OMDB_CACHE[cache_key] = {}
    save_omdb_cache()
    print(f" Movie not found: {title}")
    return {}



def main():
    # Load CSVs
    movies = pd.read_csv("movies.csv")
    ratings = pd.read_csv("ratings.csv")

    # Parse title and year
    movies[["clean_title", "year"]] = movies["title"].apply(lambda t: pd.Series(parse_title_year(t)))
    movies["genres_list"] = movies["genres"].fillna("").apply(lambda s: [g.strip() for g in s.split("|") if g.strip()])

    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        # Run schema
        with open("schema.sql", "r", encoding="utf-8") as f:
            schema_sql = f.read()
        conn.connection.executescript(schema_sql)

        # Loop through each movie and enrich with API
        for i, row in movies.head(29).iterrows():
            title = row["clean_title"]
            year = row["year"]
            movie_id = int(row["movieId"])
            omdb = fetch_omdb_details(title, year)
            time.sleep(0.1)  # Avoid hitting rate limit


            # Extract OMDb details (if available)
            movie_data = {
                "movieId": movie_id,
                "title": title,
                "year": int(year) if pd.notna(year) else None,
                "imdb_id": omdb.get("imdbID"),
                "director": omdb.get("Director"),
                "plot": omdb.get("Plot"),
                "box_office": omdb.get("BoxOffice"),
                "runtime": None,
                "language": omdb.get("Language"),
                "country": omdb.get("Country"),
            }

            # Convert runtime (e.g., "142 min" → 142)
            runtime_str = omdb.get("Runtime")
            if runtime_str and "min" in runtime_str:
                try:
                    movie_data["runtime"] = int(runtime_str.replace(" min", ""))
                except:
                    movie_data["runtime"] = None

            # Insert or update movie
            conn.execute(text("""
                INSERT OR REPLACE INTO movies 
                (movieId, title, year, imdb_id, director, plot, box_office, runtime, language, country)
                VALUES (:movieId, :title, :year, :imdb_id, :director, :plot, :box_office, :runtime, :language, :country)
            """), movie_data)

            # Insert genres
            for g in row["genres_list"]:
                conn.execute(text("INSERT OR IGNORE INTO genres (genre) VALUES (:g)"), {"g": g})
                gid = conn.execute(text("SELECT genre_id FROM genres WHERE genre=:g"), {"g": g}).fetchone()[0]
                conn.execute(text("INSERT OR IGNORE INTO movie_genres (movieId, genre_id) VALUES (:m, :gid)"),
                             {"m": movie_id, "gid": gid})

         #  Load ratings (only for existing movies to avoid foreign key errors)
        ratings = ratings.drop_duplicates(subset=["userId", "movieId", "rating", "timestamp"])

        # Select only movieIds that exist in the movies table
        existing_ids = pd.read_sql("SELECT movieId FROM movies", conn)["movieId"].unique()
        ratings = ratings[ratings["movieId"].isin(existing_ids)]

        # Insert filtered ratings into the database
        ratings.to_sql("ratings", conn, if_exists="append", index=False)

        print(f"✅ Inserted {len(ratings)} ratings (only for existing movies).")


    print("✅ ETL completed successfully! Enriched data stored in movies.db")

if __name__ == "__main__":
    main()
