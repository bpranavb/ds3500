"""Panel dashboard for the Netflix / TMDB / Rotten Tomatoes project."""

from pathlib import Path

import pandas as pd
import panel as pn

from viz import (
    fig_avg_scores_by_type,
    fig_titles_by_year,
    fig_top_titles_by_country,
)

pn.extension("plotly", "tabulator")

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_PATH = BASE_DIR / "data" / "processed" / "featured_titles.csv"


def load_dashboard_data(csv_path: Path) -> pd.DataFrame:
    """Load processed dataset for dashboard use."""
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Could not find dashboard input file at: {csv_path}\n"
            "Make sure feature_engineering creates data/processed/featured_titles.csv"
        )

    df = pd.read_csv(csv_path).copy()

    expected_columns = {
        "title",
        "type",
        "release_year",
        "country",
        "tmdb_popularity",
        "tmdb_vote_average",
        "tomatometer_score",
        "audience_score",
        "hype_gap",
    }

    missing = expected_columns - set(df.columns)
    if missing:
        raise ValueError(
            f"featured_titles.csv is missing required dashboard columns: {missing}"
        )

    df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
    df["tmdb_popularity"] = pd.to_numeric(df["tmdb_popularity"], errors="coerce")
    df["tmdb_vote_average"] = pd.to_numeric(df["tmdb_vote_average"], errors="coerce")
    df["tomatometer_score"] = pd.to_numeric(df["tomatometer_score"], errors="coerce")
    df["audience_score"] = pd.to_numeric(df["audience_score"], errors="coerce")
    df["hype_gap"] = pd.to_numeric(df["hype_gap"], errors="coerce")

    df["country"] = df["country"].fillna("Unknown").astype(str)
    df["type"] = df["type"].fillna("Unknown").astype(str)
    df["title"] = df["title"].fillna("Untitled").astype(str)

    return df


def split_countries(country_value: str) -> list[str]:
    """Split a comma-separated country string into individual countries."""
    if pd.isna(country_value):
        return ["Unknown"]

    parts = [part.strip() for part in str(country_value).split(",")]
    parts = [part for part in parts if part]

    return parts if parts else ["Unknown"]


FULL_DF = load_dashboard_data(DATA_PATH)

# Keep rows that actually have TMDB data so filters and charts stay aligned.
DF = FULL_DF[FULL_DF["tmdb_vote_average"].notna()].copy()

if DF.empty:
    raise ValueError(
        "No rows with TMDB data were found. Re-run TMDB acquisition and processing."
    )

MIN_YEAR = int(DF["release_year"].dropna().min())
MAX_YEAR = int(DF["release_year"].dropna().max())

METRIC_OPTIONS = [
    ("TMDB Vote Average", "tmdb_vote_average"),
    ("Tomatometer", "tomatometer_score"),
    ("Audience Score", "audience_score"),
    ("Hype Gap", "hype_gap"),
]

type_options = ["All"] + sorted(DF["type"].dropna().unique().tolist())

all_countries = sorted(
    {
        country
        for value in DF["country"]
        for country in split_countries(value)
    }
)
country_options = ["All"] + all_countries

type_w = pn.widgets.Select(name="Type", options=type_options, value="All")
country_w = pn.widgets.Select(name="Country", options=country_options, value="All")
year_w = pn.widgets.IntRangeSlider(
    name="Release Year Range",
    start=MIN_YEAR,
    end=MAX_YEAR,
    value=(MIN_YEAR, MAX_YEAR),
)
metric_w = pn.widgets.Select(
    name="Score Metric",
    options=METRIC_OPTIONS,
    value="tmdb_vote_average",
)
top_n_w = pn.widgets.IntSlider(name="Top N Titles", start=5, end=20, value=10)
show_table_w = pn.widgets.Checkbox(name="Show filtered table", value=True)

player_w = pn.widgets.Player(
    name="Year Player",
    start=MIN_YEAR,
    end=MAX_YEAR,
    value=MAX_YEAR,
    step=1,
    interval=700,
    loop_policy="loop",
    visible=False,
)

year_display_w = pn.widgets.IntSlider(
    name="Animated Year",
    start=MIN_YEAR,
    end=MAX_YEAR,
    value=MAX_YEAR,
)

player_w.link(year_display_w, value="value")
year_display_w.link(player_w, value="value")


def filtered_df(content_type, country, year_range):
    """Return filtered dataframe based on widget selections."""
    start_year, end_year = year_range
    dff = DF.copy()

    dff = dff[
        (dff["release_year"] >= start_year) &
        (dff["release_year"] <= end_year)
    ]

    if content_type != "All":
        dff = dff[dff["type"] == content_type]

    if country != "All":
        dff = dff[
            dff["country"].apply(
                lambda value: country in split_countries(value)
            )
        ]

    return dff.reset_index(drop=True)


def metric_with_fallback(dff: pd.DataFrame, metric: str) -> str:
    """Use selected metric if it has data, otherwise fall back to TMDB."""
    if metric in dff.columns and dff[metric].notna().any():
        return metric
    return "tmdb_vote_average"


def safe_avg(series: pd.Series) -> float:
    """Return rounded average or 0 when no values exist."""
    clean = series.dropna()
    if clean.empty:
        return 0
    return round(float(clean.mean()), 2)


@pn.depends(type_w, country_w, year_w)
def kpis(content_type, country, year_range):
    """Render KPI cards."""
    dff = filtered_df(content_type, country, year_range)

    total_titles = int(len(dff))
    avg_tmdb = safe_avg(dff["tmdb_vote_average"])
    avg_tomato = safe_avg(dff["tomatometer_score"])
    avg_audience = safe_avg(dff["audience_score"])
    avg_hype_gap = safe_avg(dff["hype_gap"])

    return pn.Row(
        pn.indicators.Number(name="Total Titles", value=total_titles, format="{value}"),
        pn.indicators.Number(name="Avg TMDB", value=avg_tmdb, format="{value}"),
        pn.indicators.Number(name="Avg Tomatometer", value=avg_tomato, format="{value}"),
        pn.indicators.Number(name="Avg Audience", value=avg_audience, format="{value}"),
        pn.indicators.Number(name="Avg Hype Gap", value=avg_hype_gap, format="{value}"),
        sizing_mode="stretch_width",
    )


@pn.depends(type_w, country_w, year_w, metric_w)
def overview_note(content_type, country, year_range, metric):
    """Show a note when the selected metric has no data and TMDB is used instead."""
    dff = filtered_df(content_type, country, year_range)
    chosen_metric = metric_with_fallback(dff, metric)

    if chosen_metric != metric:
        return pn.pane.Markdown(
            "Selected metric has little or no data in this slice, so the chart is showing TMDB Vote Average instead."
        )
    return pn.pane.Markdown("")


@pn.depends(type_w, country_w, year_w, metric_w)
def overview_plot(content_type, country, year_range, metric):
    """Overview chart."""
    dff = filtered_df(content_type, country, year_range)
    chosen_metric = metric_with_fallback(dff, metric)
    return fig_avg_scores_by_type(dff, chosen_metric)


@pn.depends(type_w, country_w, year_w, metric_w)
def drilldown_note(content_type, country, year_range, metric):
    """Show a note when the selected metric has no data and TMDB is used instead."""
    dff = filtered_df(content_type, country, year_range)
    chosen_metric = metric_with_fallback(dff, metric)

    if chosen_metric != metric:
        return pn.pane.Markdown(
            "Selected metric has little or no data in this slice, so the chart is showing TMDB Vote Average instead."
        )
    return pn.pane.Markdown("")


@pn.depends(type_w, country_w, year_w, metric_w, top_n_w)
def drilldown_plot(content_type, country, year_range, metric, top_n):
    """Drill-down chart."""
    dff = filtered_df(content_type, country, year_range)
    chosen_metric = metric_with_fallback(dff, metric)
    return fig_top_titles_by_country(dff, chosen_metric, int(top_n))


@pn.depends(type_w, country_w, year_w)
def animated_note(content_type, country, year_range):
    """Explain the animated chart."""
    dff = filtered_df(content_type, country, year_range)
    if dff.empty:
        return pn.pane.Markdown("No titles match this selection.")
    return pn.pane.Markdown(
        "Use Play to move through years. This chart shows the cumulative number of Movie and TV Show titles through the selected year."
    )


@pn.depends(type_w, country_w, year_w, year_display_w)
def animated_plot(content_type, country, year_range, animated_year):
    """Animated chart controlled by Panel player."""
    dff = filtered_df(content_type, country, year_range)

    start_year, end_year = year_range
    safe_year = min(max(int(animated_year), int(start_year)), int(end_year))

    return fig_titles_by_year(dff, safe_year)


@pn.depends(show_table_w, type_w, country_w, year_w)
def data_table(show_table, content_type, country, year_range):
    """Optional filtered table."""
    if not show_table:
        return pn.pane.Markdown("")

    dff = filtered_df(content_type, country, year_range)

    if dff.empty:
        return pn.pane.Markdown("No titles match this selection.")

    cols = [
        "title",
        "type",
        "release_year",
        "country",
        "tmdb_vote_average",
        "tomatometer_score",
        "audience_score",
        "hype_gap",
    ]

    existing_cols = [col for col in cols if col in dff.columns]
    return pn.widgets.Tabulator(
        dff[existing_cols],
        pagination="remote",
        page_size=12,
        height=350,
    )


controls = pn.Card(
    pn.pane.Markdown("## Filters"),
    type_w,
    country_w,
    year_w,
    metric_w,
    top_n_w,
    show_table_w,
    width=350,
)

animated_controls = pn.Row(
    player_w,
    year_display_w,
)

tab_overview = pn.Column(
    pn.pane.Markdown("## Overview"),
    kpis,
    overview_note,
    overview_plot,
    sizing_mode="stretch_width",
)

tab_drilldown = pn.Column(
    pn.pane.Markdown("## Drill-down"),
    drilldown_note,
    drilldown_plot,
    data_table,
    sizing_mode="stretch_width",
)

tab_animated = pn.Column(
    pn.pane.Markdown("## Animated Trends"),
    animated_note,
    animated_plot,
    animated_controls,
    sizing_mode="stretch_width",
)

tabs = pn.Tabs(
    ("Overview", tab_overview),
    ("Drill-down", tab_drilldown),
    ("Animated", tab_animated),
)

pn.template.MaterialTemplate(
    title="DataFlix Dashboard",
    main=[pn.Row(controls, tabs)],
).servable()
