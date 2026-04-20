"""Plotly visualizations for the Netflix / TMDB / Rotten Tomatoes dashboard."""

import pandas as pd
import plotly.express as px

METRIC_LABELS = {
    "tmdb_vote_average": "TMDB Vote Average",
    "tomatometer_score": "Tomatometer Score",
    "audience_score": "Audience Score",
    "hype_gap": "Hype Gap",
}

TYPE_COLORS = {
    "Movie": "#4F46E5",
    "TV Show": "#F97316",
    "Unknown": "#9CA3AF",
}


def fig_avg_scores_by_type(df: pd.DataFrame, metric: str):
    """Create overview bar chart of average metric by content type."""
    if df.empty or metric not in df.columns:
        return px.bar(title="Overview (no data)")

    dff = (
        df.dropna(subset=[metric, "type"])
        .groupby("type", dropna=False)[metric]
        .mean()
        .reset_index()
        .sort_values(metric, ascending=False)
    )

    if dff.empty:
        return px.bar(title="Overview (no data)")

    fig = px.bar(
        dff,
        x="type",
        y=metric,
        color="type",
        color_discrete_map=TYPE_COLORS,
        title=f"Average {METRIC_LABELS.get(metric, metric)} by Type",
        labels={
            "type": "Type",
            metric: METRIC_LABELS.get(metric, metric),
        },
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=55, b=10),
        showlegend=False,
    )
    return fig


def fig_top_titles_by_country(df: pd.DataFrame, metric: str, top_n: int):
    """Create drill-down chart of top titles for current selection."""
    if df.empty or metric not in df.columns:
        return px.bar(title="Drill-down (no data)")

    dff = (
        df.dropna(subset=[metric, "title", "type"])
        .sort_values(metric, ascending=False)
        .head(top_n)
    )

    if dff.empty:
        return px.bar(title="Drill-down (no data)")

    fig = px.bar(
        dff,
        x=metric,
        y="title",
        color="type",
        color_discrete_map=TYPE_COLORS,
        orientation="h",
        title=f"Top {top_n} Titles by {METRIC_LABELS.get(metric, metric)}",
        labels={
            metric: METRIC_LABELS.get(metric, metric),
            "title": "Title",
            "type": "Type",
        },
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=55, b=10),
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def fig_titles_by_year(df: pd.DataFrame, year: int):
    """Create a cumulative bar chart up to a selected year."""
    if df.empty:
        return px.bar(title="Animated chart (no data)")

    dff = df.copy()
    dff["release_year"] = pd.to_numeric(dff["release_year"], errors="coerce")
    dff = dff.dropna(subset=["release_year", "type"])

    if dff.empty:
        return px.bar(title="Animated chart (no data)")

    dff["release_year"] = dff["release_year"].astype(int)
    dff = dff[dff["release_year"] <= int(year)]

    if dff.empty:
        return px.bar(title=f"No titles up to {year}")

    counts = (
        dff.groupby("type", dropna=False)
        .size()
        .reset_index(name="title_count")
        .sort_values("title_count", ascending=False)
    )

    fig = px.bar(
        counts,
        x="type",
        y="title_count",
        color="type",
        color_discrete_map=TYPE_COLORS,
        title=f"Cumulative Number of Titles by Type Through {year}",
        labels={
            "type": "Type",
            "title_count": "Number of Titles",
        },
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=55, b=10),
        showlegend=False,
        xaxis_title="Type",
        yaxis_title="Number of Titles",
    )
    return fig
