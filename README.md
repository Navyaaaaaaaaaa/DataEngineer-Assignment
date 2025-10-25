# Building a Simple Movie Data Pipeline

## Overview

This project, **Building a Simple Movie Data Pipeline**, demonstrates an end-to-end Extract, Transform, and Load (ETL) process designed to integrate and enrich movie data from multiple sources. The pipeline reads movie and ratings data from CSV files, enriches it with additional metadata using the **OMDb API**, and stores the structured data in an **SQLite** database for analytics use.

The primary objectives of this project are:

* To design a simple, modular, and extensible ETL pipeline.
* To demonstrate integration of external APIs for data enrichment.
* To manage and store enriched data in a structured relational database.

---

## Environment Setup

### Prerequisites

* **Python 3.10+** installed on my system.
* Internet access (required for OMDb API calls).

### Required Libraries

Install dependencies using the following command:

```bash
pip install pandas requests sqlalchemy rapidfuzz unidecode
```

### OMDb API Key Setup

1. Visit the OMDb API website: [http://www.omdbapi.com/](http://www.omdbapi.com/)
2. Generate a free API key.
3. Set the key as an environment variable in your terminal:

   Open VS Code Terminal (PowerShell or Command Prompt).
   Run this PowerShell command (note the $env: syntax):

   ```bash
   setx OMDB_API_KEY "c54f36a8"

  You should see:
    SUCCESS: Specified value was saved.
    
 Close your VS Code terminal completely
 (this is required — setx only affects new sessions).

 Reopen a new VS Code terminal, then check:
   echo $env:OMDB_API_KEY

 Now you will see your key
   ```

  

---

## Project Structure

```
DataEngineer-Assignment/
│
├── etl.py            # Main ETL pipeline script
├── schema.sql        # Database schema definition
├── queries.sql       # SQL queries for analytics validation
├── movies.csv        # Movie dataset
├── ratings.csv       # Ratings dataset
└── README.md         # Project documentation
```

---

## Execution Steps

### 1. Initialize the Database

Ensure that `schema.sql` is in the same directory as `etl.py`. It defines the relational structure for movies, genres, and ratings.

### 2. Run the ETL Pipeline

Execute the following command to run the pipeline:

```bash
python etl.py
```

The pipeline performs the following operations:

* **Extracts** data from `movies.csv` and `ratings.csv`.
* **Enriches** each movie record with details (Director, Plot, Runtime, etc.) using the OMDb API.
* **Loads** the enriched dataset into the `movies.db` SQLite database.

If an older version of `movies.db` exists, it should be deleted before rerunning the pipeline to avoid conflicts.

### 3. Verify the Data

To validate or explore the database, open `movies.db` using an SQLite viewer or execute SQL queries from the terminal:

```bash
sqlite3 movies.db < queries.sql
```
* For connecting database right click on `movies.db` and select "connect database".Now it will show your database table.
* For running queries select one query from `queries.sql` and right clik on selected query then select Run query. Now it will run the query which you have selected.
---

## Design Choices and Assumptions

1. **SQLite Database:** Selected for simplicity and portability. It is lightweight, file-based, and ideal for local analytics testing.
2. **OMDb API Integration:** Movie data is enriched using the OMDb API. The script handles fuzzy title matching and caching to reduce API calls.
3. **Caching Strategy:** A local JSON cache (`omdb_cache.json`) stores previously retrieved API responses, minimizing redundant API requests.
4. **Data Cleaning:** Titles are normalized (e.g., handling trailing commas, accents, and punctuation) to improve match accuracy.
5. **Error Handling:** The ETL gracefully handles missing or incomplete API responses and logs them without interrupting execution.
6. **Foreign Key Integrity:** The schema enforces foreign key relationships between movies, genres, and ratings to ensure data consistency.

---

## Challenges and Solutions

| **Challenge**       | **Description**                                                   | **Solution Implemented**                                                           |
| ------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| API Rate Limiting   | OMDb API restricts frequent calls per second.                     | Implemented a short sleep (`time.sleep(0.25)`) between requests and local caching. |
| Title Mismatches    | Movie titles in the CSV differ from OMDb API titles.              | Applied fuzzy string matching (using `rapidfuzz`) and title normalization.         |
| Database Locking    | Occurred when re-running pipeline on an open DB.                  | Ensured the database is closed before reruns or deleted before pipeline execution. |
| Foreign Key Errors  | Ratings referred to missing movies when only a subset was loaded. | Loaded all movies before inserting ratings to maintain referential integrity.      |
| Missing Title Field | Some API calls returned incomplete data.                          | Added a fallback to use the CSV title when the API response lacked a title.        |

---

## Results

After successful execution, the `movies.db` database contains:

* **movies** table with enriched metadata (title, year, director, plot, etc.)
* **genres** and **movie_genres** mapping tables
* **ratings** table with user ratings and timestamps

This structured dataset enables the analytics team to query and analyze movie performance, ratings distribution, and trends.

---




