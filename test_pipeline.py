import pytest
import pandas as pd
import re
from unittest.mock import patch, MagicMock


def clean_title(title: str) -> str:
    if not isinstance(title, str):
        return ""
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", "", title)
    title = title.strip()
    return title


def load_csv(filepath: str) -> pd.DataFrame:
    return pd.read_csv(filepath)


def merge_datasets(netflix_df: pd.DataFrame, tmdb_df: pd.DataFrame) -> pd.DataFrame:
    return netflix_df.merge(tmdb_df, on="title", how="left")


class TestCleanTitle:

    def test_removes_special_characters(self):
        assert clean_title("Spider-Man: No Way Home!") == "spiderman no way home"

    def test_converts_to_lowercase(self):
        assert clean_title("THE DARK KNIGHT") == "the dark knight"

    def test_handles_leading_trailing_whitespace(self):
        assert clean_title("  Inception  ") == "inception"

    def test_handles_empty_string(self):
        assert clean_title("") == ""

    def test_handles_non_string_input(self):
        assert clean_title(None) == ""
        assert clean_title(float("nan")) == ""

    def test_numbers_preserved(self):
        result = clean_title("Ocean's 11!")
        assert "11" in result

    def test_ampersand_removed(self):
        assert "&" not in clean_title("Jekyll & Hyde")


class TestDataLoading:

    def test_load_csv_returns_dataframe(self, tmp_path):
        sample = tmp_path / "sample.csv"
        sample.write_text("title,year\nInception,2010\nDune,2021\n")
        df = load_csv(str(sample))
        assert isinstance(df, pd.DataFrame)

    def test_loaded_csv_is_not_empty(self, tmp_path):
        sample = tmp_path / "movies.csv"
        sample.write_text("title,year\nInception,2010\n")
        df = load_csv(str(sample))
        assert len(df) > 0

    def test_loaded_csv_has_expected_columns(self, tmp_path):
        sample = tmp_path / "netflix_titles.csv"
        sample.write_text("title,type,release_year\nInception,Movie,2010\n")
        df = load_csv(str(sample))
        for col in ["title", "type", "release_year"]:
            assert col in df.columns

    def test_missing_file_raises_error(self):
        with pytest.raises(FileNotFoundError):
            load_csv("data/raw/this_file_does_not_exist.csv")


class TestMergeLogic:

    @pytest.fixture
    def netflix_df(self):
        return pd.DataFrame({
            "title": ["inception", "dune", "parasite"],
            "type":  ["Movie", "Movie", "Movie"],
        })

    @pytest.fixture
    def tmdb_df(self):
        return pd.DataFrame({
            "title":      ["inception", "dune"],
            "tmdb_score": [8.8, 7.9],
        })

    def test_left_join_preserves_all_netflix_rows(self, netflix_df, tmdb_df):
        merged = merge_datasets(netflix_df, tmdb_df)
        assert len(merged) == len(netflix_df)

    def test_left_join_fills_missing_tmdb_with_nan(self, netflix_df, tmdb_df):
        merged = merge_datasets(netflix_df, tmdb_df)
        no_match = merged[merged["title"] == "parasite"]
        assert no_match["tmdb_score"].isna().all()

    def test_merged_dataframe_contains_both_source_columns(self, netflix_df, tmdb_df):
        merged = merge_datasets(netflix_df, tmdb_df)
        for col in ["title", "type", "tmdb_score"]:
            assert col in merged.columns

    def test_no_duplicate_rows_after_merge(self, netflix_df, tmdb_df):
        merged = merge_datasets(netflix_df, tmdb_df)
        assert merged.duplicated(subset=["title"]).sum() == 0

    def test_merge_on_empty_tmdb_returns_all_netflix_rows(self, netflix_df):
        empty_tmdb = pd.DataFrame(columns=["title", "tmdb_score"])
        merged = merge_datasets(netflix_df, empty_tmdb)
        assert len(merged) == len(netflix_df)
