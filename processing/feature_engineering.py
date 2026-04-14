import pandas as pd


def calculate_hype_gap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the Hype Gap for each title.

    The Hype Gap is defined as TMDB Popularity minus the Rotten Tomatoes
    score. A high positive value means the title is overhyped relative to
    its critical reception. A negative value indicates a hidden gem.

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

    The score is computed as the audience rating divided by the number of
    seasons. A higher score means strong ratings even across many seasons,
    indicating a highly bingeable show.

    Args:
        df (pd.DataFrame): Merged DataFrame containing 'audience_rating'
                           and 'num_seasons' columns.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'binge_score' column.
                      Rows with zero or missing seasons are set to NaN.
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

    A title is considered a hidden gem if its hype_gap is below the given
    threshold, meaning it is critically well-received but not very popular.

    Args:
        df (pd.DataFrame): DataFrame containing a 'hype_gap' column.
        threshold (float): Hype Gap value below which a title is flagged.
                           Defaults to -20.0.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'hidden_gem' boolean column.
    """
    df = df.copy()
    df["hidden_gem"] = df["hype_gap"] < threshold
    return df


def flag_overhyped(df: pd.DataFrame, threshold: float = 20.0) -> pd.DataFrame:
    """
    Flags titles as overhyped based on their Hype Gap.

    A title is considered overhyped if its hype_gap exceeds the given
    threshold, meaning it is very popular but not critically acclaimed.

    Args:
        df (pd.DataFrame): DataFrame containing a 'hype_gap' column.
        threshold (float): Hype Gap value above which a title is flagged.
                           Defaults to 20.0.

    Returns:
        pd.DataFrame: Original DataFrame with a new 'overhyped' boolean column.
    """
    df = df.copy()
    df["overhyped"] = df["hype_gap"] > threshold
    return df


def run_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs all feature engineering steps on the merged DataFrame.

    Applies Hype Gap calculation, Binge-ability Score, hidden gem flagging,
    and overhyped flagging in sequence.

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
