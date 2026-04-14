import pandas as pd


def calculate_hype_gap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the Hype Gap for each title.

    Args:
        df (pd.DataFrame): Merged DataFrame containing 'tmdb_popularity'
                           and 'rt_score' columns.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'hype_gap' column.
    """
    df = df.copy()
    df["hype_gap"] = df["tmdb_popularity"] - df["rt_score"]
    return df


def calculate_binge_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates a Binge-ability Score for TV shows.

    Args:
        df (pd.DataFrame): Merged DataFrame containing 'audience_rating'
                           and 'num_seasons' columns.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'binge_score' column.
    """
    df = df.copy()
    df["binge_score"] = df.apply(
        lambda row: row["audience_rating"] / row["num_seasons"]
        if pd.notna(row["num_seasons"]) and row["num_seasons"] > 0
        else None,
        axis=1
    )
    return df


def flag_hidden_gems(df: pd.DataFrame, threshold: float = -20.0) -> pd.DataFrame:
    """
    Flags titles as hidden gems based on their Hype Gap.

    Args:
        df (pd.DataFrame): DataFrame containing a 'hype_gap' column.
        threshold (float): Hype Gap value below which a title is flagged.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'hidden_gem' boolean column.
    """
    df = df.copy()
    df["hidden_gem"] = df["hype_gap"] < threshold
    return df


def flag_overhyped(df: pd.DataFrame, threshold: float = 20.0) -> pd.DataFrame:
    """
    Flags titles as overhyped based on their Hype Gap.

    Args:
        df (pd.DataFrame): DataFrame containing a 'hype_gap' column.
        threshold (float): Hype Gap value above which a title is flagged.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'overhyped' boolean column.
    """
    df = df.copy()
    df["overhyped"] = df["hype_gap"] > threshold
    return df


def run_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs all feature engineering steps on the merged DataFrame.

    Args:
        df (pd.DataFrame): Merged DataFrame with required columns.

    Returns:
        pd.DataFrame: DataFrame with all engineered feature columns added.
    """
    df = calculate_hype_gap(df)
    df = calculate_binge_score(df)
    df = flag_hidden_gems(df)
    df = flag_overhyped(df)
    return df


if __name__ == "__main__":
    sample = pd.DataFrame({
        "title":           ["Inception", "Stranger Things", "Cats"],
        "tmdb_popularity": [80.0, 95.0, 30.0],
        "rt_score":        [87.0, 93.0, 20.0],
        "audience_rating": [90.0, 95.0, 25.0],
        "num_seasons":     [None, 4.0, None],
    })

    result = run_feature_engineering(sample)
    print(result[["title", "hype_gap", "binge_score", "hidden_gem", "overhyped"]])
