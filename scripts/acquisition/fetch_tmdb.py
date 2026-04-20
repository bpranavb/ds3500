"""Fetch TMDB metadata for Netflix titles."""

from pathlib import Path
import time
import requests
import pandas as pd
from requests.exceptions import RequestException

SEARCH_MOVIE_URL = "https://api.themoviedb.org/3/search/movie"
SEARCH_TV_URL = "https://api.themoviedb.org/3/search/tv"

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"

NETFLIX_PATH = RAW_DIR / "netflix_titles.csv"
OUTPUT_PATH = RAW_DIR / "tmdb_raw.csv"

REQUEST_DELAY = 0.10
TIMEOUT = 8
TEST_LIMIT = None
MAX_PAGES = 2

API_KEY = "PASTE_YOUR_REAL_TMDB_KEY_HERE"


def load_netflix_titles(csv_path: Path) -> pd.DataFrame:
    """Load Netflix dataset and keep only title/type columns."""
    print(f"Looking for Netflix file at: {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find Netflix file at: {csv_path}\n"
            "Put netflix_titles.csv inside data/raw/."
        )

    df = pd.read_csv(csv_path)
    print(f"Raw Netflix shape: {df.shape}")

    required_columns = {"title", "type"}
    if not required_columns.issubset(df.columns):
        raise ValueError(
            "netflix_titles.csv must contain 'title' and 'type' columns."
        )

    df = df[["title", "type"]].dropna(subset=["title", "type"]).drop_duplicates()
    print(f"Cleaned Netflix shape: {df.shape}")

    if TEST_LIMIT is not None:
        df = df.head(TEST_LIMIT)
        print(f"Using TEST_LIMIT={TEST_LIMIT}, shape now: {df.shape}")

    if df.empty:
        raise ValueError("Netflix dataframe is empty after cleaning.")

    print(df.head())
    return df


def search_tmdb(
    title: str,
    content_type: str,
    session: requests.Session,
    api_key: str,
) -> dict | None:
    """Search TMDB for one title and return the top result."""
    normalized_type = content_type.strip().lower()
    url = SEARCH_MOVIE_URL if normalized_type == "movie" else SEARCH_TV_URL

    all_results = []

    for page in range(1, MAX_PAGES + 1):
        params = {
            "api_key": api_key,
            "query": title,
            "page": page,
        }

        try:
            response = session.get(url, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except RequestException as exc:
            print(f"Request failed for '{title}' on page {page}: {exc}")
            break

        page_results = data.get("results", [])
        if not page_results:
            break

        all_results.extend(page_results)
        time.sleep(REQUEST_DELAY)

    if not all_results:
        return None

    item = all_results[0]

    if normalized_type == "movie":
        return {
            "source_title": title,
            "content_type": content_type,
            "tmdb_id": item.get("id"),
            "title": item.get("title"),
            "popularity": item.get("popularity"),
            "vote_average": item.get("vote_average"),
            "release_date": item.get("release_date"),
        }

    return {
        "source_title": title,
        "content_type": content_type,
        "tmdb_id": item.get("id"),
        "title": item.get("name"),
        "popularity": item.get("popularity"),
        "vote_average": item.get("vote_average"),
        "release_date": item.get("first_air_date"),
    }


def fetch_tmdb_data(netflix_df: pd.DataFrame, api_key: str) -> pd.DataFrame:
    """Fetch TMDB data for each Netflix title."""
    all_rows = []

    with requests.Session() as session:
        for _, row in netflix_df.iterrows():
            title = str(row["title"]).strip()
            content_type = str(row["type"]).strip()

            print(f"Fetching TMDB: {title} ({content_type})")
            result = search_tmdb(title, content_type, session, api_key)

            if result is not None:
                all_rows.append(result)

    tmdb_df = pd.DataFrame(all_rows)
    print(f"TMDB output shape: {tmdb_df.shape}")
    return tmdb_df


def main() -> None:
    """Run TMDB acquisition pipeline."""
    if API_KEY == "PASTE_YOUR_REAL_TMDB_KEY_HERE" or not API_KEY.strip():
        raise ValueError('Paste your real TMDB key into API_KEY = "..." first.')

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    netflix_df = load_netflix_titles(NETFLIX_PATH)
    tmdb_df = fetch_tmdb_data(netflix_df, API_KEY)

    if tmdb_df.empty:
        raise ValueError("TMDB output dataframe is empty.")

    try:
        tmdb_df.to_csv(OUTPUT_PATH, index=False)
        print(f"Saved TMDB raw data to {OUTPUT_PATH}")
    except PermissionError:
        backup_path = RAW_DIR / "tmdb_raw_backup.csv"
        tmdb_df.to_csv(backup_path, index=False)
        print(f"tmdb_raw.csv was locked, so data was saved to {backup_path}")


if __name__ == "__main__":
    main()
