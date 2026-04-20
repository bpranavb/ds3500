"""Merge raw Netflix, TMDB, and Rotten Tomatoes data into one processed file."""

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

NETFLIX_PATH = RAW_DIR / "netflix_titles.csv"
TMDB_PATH = RAW_DIR / "tmdb_raw.csv"
RT_PATH = RAW_DIR / "rotten_tomatoes_raw.csv"
OUTPUT_PATH = PROCESSED_DIR / "featured_titles.csv"


def load_csv(path: Path, name: str) -> pd.DataFrame:
    """Load one CSV file."""
    if not path.exists():
        raise FileNotFoundError(f"Could not find {name} at: {path}")
    df = pd.read_csv(path)
    print(f"{name} shape: {df.shape}")
    return df


def prepare_netflix(df: pd.DataFrame) -> pd.DataFrame:
    """Keep useful Netflix columns and clean titles."""
    required = {"title", "type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Netflix file missing columns: {missing}")

    keep = ["title", "type"]
    optional = ["release_year", "country"]
    for col in optional:
        if col in df.columns:
            keep.append(col)

    out = df[keep].copy()
    out["title"] = out["title"].astype(str).str.strip()
    out["type"] = out["type"].astype(str).str.strip()
    out = out.dropna(subset=["title", "type"]).drop_duplicates(subset=["title", "type"])
    return out


def prepare_tmdb(df: pd.DataFrame) -> pd.DataFrame:
    """Rename TMDB columns to dashboard-friendly names."""
    required = {"source_title", "content_type", "popularity", "vote_average"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"TMDB file missing columns: {missing}")

    out = df.copy()
    out["source_title"] = out["source_title"].astype(str).str.strip()
    out["content_type"] = out["content_type"].astype(str).str.strip()

    out = out.rename(
        columns={
            "content_type": "type",
            "title": "tmdb_title",
            "popularity": "tmdb_popularity",
            "vote_average": "tmdb_vote_average",
        }
    )

    cols = [
        "source_title",
        "type",
        "tmdb_id",
        "tmdb_title",
        "tmdb_popularity",
        "tmdb_vote_average",
        "release_date",
    ]
    existing = [col for col in cols if col in out.columns]
    out = out[existing].drop_duplicates(subset=["source_title", "type"])
    return out


def prepare_rt(df: pd.DataFrame) -> pd.DataFrame:
    """Clean Rotten Tomatoes columns."""
    required = {"source_title", "tomatometer_score", "audience_score"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"RT file missing columns: {missing}")

    out = df.copy()
    out["source_title"] = out["source_title"].astype(str).str.strip()
    out["tomatometer_score"] = pd.to_numeric(out["tomatometer_score"], errors="coerce")
    out["audience_score"] = pd.to_numeric(out["audience_score"], errors="coerce")

    if "title" in out.columns:
        out = out.rename(columns={"title": "rt_title"})
    if "year" in out.columns:
        out = out.rename(columns={"year": "rt_year"})

    cols = [
        "source_title",
        "rt_title",
        "rt_year",
        "tomatometer_score",
        "audience_score",
    ]
    existing = [col for col in cols if col in out.columns]
    out = out[existing].drop_duplicates(subset=["source_title"])
    return out


def build_featured_titles(
    netflix_df: pd.DataFrame,
    tmdb_df: pd.DataFrame,
    rt_df: pd.DataFrame,
) -> pd.DataFrame:
    """Merge all sources and create dashboard features."""
    featured = netflix_df.merge(
        tmdb_df,
        how="left",
        left_on=["title", "type"],
        right_on=["source_title", "type"],
    )

    featured = featured.merge(
        rt_df,
        how="left",
        left_on="title",
        right_on="source_title",
    )

    featured["tmdb_popularity"] = pd.to_numeric(
        featured.get("tmdb_popularity"), errors="coerce"
    )
    featured["tmdb_vote_average"] = pd.to_numeric(
        featured.get("tmdb_vote_average"), errors="coerce"
    )
    featured["tomatometer_score"] = pd.to_numeric(
        featured.get("tomatometer_score"), errors="coerce"
    )
    featured["audience_score"] = pd.to_numeric(
        featured.get("audience_score"), errors="coerce"
    )

    if "release_year" in featured.columns:
        featured["release_year"] = pd.to_numeric(
            featured["release_year"], errors="coerce"
        )
    else:
        featured["release_year"] = pd.to_datetime(
            featured.get("release_date"), errors="coerce"
        ).dt.year

    if "country" not in featured.columns:
        featured["country"] = "Unknown"

    featured["hype_gap"] = featured["tmdb_popularity"] - featured["tomatometer_score"]

    final_cols = [
        "title",
        "type",
        "release_year",
        "country",
        "tmdb_id",
        "tmdb_title",
        "tmdb_popularity",
        "tmdb_vote_average",
        "tomatometer_score",
        "audience_score",
        "hype_gap",
        "release_date",
        "rt_title",
        "rt_year",
    ]

    existing = [col for col in final_cols if col in featured.columns]
    featured = featured[existing].copy()

    featured["country"] = featured["country"].fillna("Unknown").astype(str)
    featured["title"] = featured["title"].fillna("Untitled").astype(str)
    featured["type"] = featured["type"].fillna("Unknown").astype(str)

    featured = featured.drop_duplicates(subset=["title", "type"]).reset_index(drop=True)
    return featured


def main() -> None:
    """Run processing pipeline."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    netflix_df = prepare_netflix(load_csv(NETFLIX_PATH, "Netflix"))
    tmdb_df = prepare_tmdb(load_csv(TMDB_PATH, "TMDB"))
    rt_df = prepare_rt(load_csv(RT_PATH, "Rotten Tomatoes"))

    featured_df = build_featured_titles(netflix_df, tmdb_df, rt_df)

    print(f"featured_titles shape: {featured_df.shape}")
    print(featured_df.head())

    featured_df.to_csv(OUTPUT_PATH, index=False)
    print(f"Saved processed data to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
