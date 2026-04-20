"""Scrape Rotten Tomatoes search results for Netflix titles."""

from pathlib import Path
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.exceptions import RequestException

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"

NETFLIX_PATH = RAW_DIR / "netflix_titles.csv"
OUTPUT_PATH = RAW_DIR / "rotten_tomatoes_raw.csv"

SEARCH_URL = "https://www.rottentomatoes.com/search"

REQUEST_DELAY = 0.25
TIMEOUT = 8
TEST_LIMIT = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

RT_COLUMNS = [
    "source_title",
    "title",
    "year",
    "tomatometer_score",
    "audience_score",
]


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


def parse_search_results(html: str, source_title: str) -> dict:
    """Parse Rotten Tomatoes search page and return the first structured result."""
    soup = BeautifulSoup(html, "html.parser")
    search_rows = soup.find_all(["search-page-media-row", "search-page-result"])

    for row in search_rows:
        title = row.get("title") or row.get("name")
        year = row.get("year")
        tomatometer = row.get("tomatometerscore")
        audience = row.get("audiencescore")

        if title:
            return {
                "source_title": source_title,
                "title": title,
                "year": year,
                "tomatometer_score": tomatometer,
                "audience_score": audience,
            }

    return {
        "source_title": source_title,
        "title": None,
        "year": None,
        "tomatometer_score": None,
        "audience_score": None,
    }


def scrape_rotten_tomatoes(title: str, session: requests.Session) -> dict:
    """Search Rotten Tomatoes for a title and return one structured row."""
    params = {"search": title}

    try:
        response = session.get(
            SEARCH_URL,
            params=params,
            headers=HEADERS,
            timeout=TIMEOUT,
        )
        response.raise_for_status()
        result = parse_search_results(response.text, title)
    except RequestException as exc:
        print(f"RT request failed for '{title}': {exc}")
        result = {
            "source_title": title,
            "title": None,
            "year": None,
            "tomatometer_score": None,
            "audience_score": None,
        }

    time.sleep(REQUEST_DELAY)
    return result


def fetch_rt_data(netflix_df: pd.DataFrame) -> pd.DataFrame:
    """Scrape Rotten Tomatoes data for each Netflix title."""
    all_rows = []

    with requests.Session() as session:
        for _, row in netflix_df.iterrows():
            title = str(row["title"]).strip()
            print(f"Scraping Rotten Tomatoes: {title}")

            result = scrape_rotten_tomatoes(title, session)
            all_rows.append(result)

    rt_df = pd.DataFrame(all_rows, columns=RT_COLUMNS)
    print(f"Rotten Tomatoes output shape: {rt_df.shape}")
    return rt_df


def main() -> None:
    """Run Rotten Tomatoes acquisition pipeline."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    netflix_df = load_netflix_titles(NETFLIX_PATH)
    rt_df = fetch_rt_data(netflix_df)

    if rt_df.empty:
        raise ValueError("Output dataframe is empty.")

    try:
        rt_df.to_csv(OUTPUT_PATH, index=False)
        print(f"Saved Rotten Tomatoes raw data to {OUTPUT_PATH}")
    except PermissionError:
        backup_path = RAW_DIR / "rotten_tomatoes_raw_backup.csv"
        rt_df.to_csv(backup_path, index=False)
        print(f"rotten_tomatoes_raw.csv was locked, so data was saved to {backup_path}")


if __name__ == "__main__":
    main()
