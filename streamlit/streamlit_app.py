import json
import os
from datetime import date
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="GigWise Analytics", layout="wide")
st.title("GigWise Analytics Dashboard")
st.caption("Concert touring intensity and setlist evolution  \n"
           "Data Engineering Zoomcamp 2026 capstone project by Lorenzo Ederone")

# ── Data loading: snapshot vs live ───────────────────────────────────
# DASHBOARD_MODE=cloud  → parquet snapshots only, no BigQuery
# DASHBOARD_MODE=local  → parquet if fresh (today), else live BigQuery
DASHBOARD_MODE = os.getenv("DASHBOARD_MODE", "local").lower()
DATA_DIR = Path(__file__).resolve().parent / "data"


def _snapshot_is_fresh() -> bool:
    """Return True if parquet snapshots exist and were exported today."""
    meta_path = DATA_DIR / "meta.json"
    if not meta_path.exists():
        return False
    try:
        meta = json.loads(meta_path.read_text())
        return meta.get("exported_date") == date.today().isoformat()
    except Exception:
        return False


def _load_parquet(name: str) -> pd.DataFrame | None:
    path = DATA_DIR / f"{name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


_use_snapshots = (DASHBOARD_MODE == "cloud") or _snapshot_is_fresh()

if _use_snapshots:
    st.caption("Data source: embedded snapshot")

    def load_df(name: str, _query: str = "") -> pd.DataFrame:
        df = _load_parquet(name)
        return df if df is not None else pd.DataFrame()

    def load_optional_df(name: str, _query: str = "") -> pd.DataFrame | None:
        return _load_parquet(name)
else:
    from google.cloud import bigquery

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

    if not project_id:
        st.error("Set GCP_PROJECT_ID in your environment before running the dashboard.")
        st.stop()

    client = bigquery.Client(project=project_id)

    @st.cache_data(ttl=1800)
    def _run_query(query: str) -> pd.DataFrame:
        return client.query(query).to_dataframe()

    def load_df(_name: str, query: str = "") -> pd.DataFrame:
        return _run_query(query)

    def load_optional_df(_name: str, query: str = "") -> pd.DataFrame | None:
        try:
            return _run_query(query)
        except Exception:
            return None

# ── Shared date range for Tile 1 ─────────────────────────────────────
_project = os.getenv("GCP_PROJECT_ID", "")
_dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")
_streaming = os.getenv("BQ_DATASET_STREAMING", "streaming")

date_range = load_df("date_range", f"""
    SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date
    FROM `{_project}.{_dataset}.fact_concert`
    WHERE source = 'ticketmaster'
""")
date_subtitle = ""
if not date_range.empty and date_range.iloc[0]["min_date"] is not None:
    d_min = date_range.iloc[0]["min_date"].strftime("%d/%m/%Y")
    d_max = date_range.iloc[0]["max_date"].strftime("%d/%m/%Y")
    date_subtitle = f"Upcoming concerts: {d_min} \u2013 {d_max}"

# ── Data queries ─────────────────────────────────────────────────────

df_repertoire = load_df("repertoire", f"""
    SELECT artist_name, primary_genre, concert_year, concert_count,
           unique_songs, total_song_performances
    FROM `{_project}.{_dataset}.mart_artist_yearly_repertoire`
    WHERE artist_name IS NOT NULL
    ORDER BY artist_name, concert_year
    LIMIT 5000
""")

df_freshness = load_df("freshness", f"""
    SELECT artist_name, primary_genre, concert_year, concert_count,
           unique_songs, first_time_songs, repeated_songs, freshness_pct
    FROM `{_project}.{_dataset}.mart_artist_setlist_freshness`
    WHERE artist_name IS NOT NULL
    ORDER BY artist_name, concert_year
    LIMIT 5000
""")

MIN_SHOWS = 5

# ── Optional data: Spark artist similarity (production only) ─────────
df_similarity = load_optional_df("similarity", f"""
    SELECT artist_a, artist_b, shared_venues, jaccard_similarity
    FROM `{_project}.{_dataset}.spark_artist_similarity`
    ORDER BY jaccard_similarity DESC
    LIMIT 5000
""")

# ── Optional data: Kafka live event stream ───────────────────────────
df_live = load_optional_df("live_events", f"""
    SELECT event_id, event_name, artist_name, event_date, event_status,
           venue_name, city, country_code, observed_at, is_new
    FROM `{_project}.{_streaming}.live_event_updates`
    ORDER BY observed_at DESC
    LIMIT 500
""")

# ── Tile 1: Artist Touring Intensity ─────────────────────────────────
# Two charts stacked vertically: top = artist bar, bottom = genre bar

def _is_stream_live() -> bool:
    """Check if the Kafka producer process is currently running."""
    pid_file = Path(__file__).resolve().parent.parent / "logs" / "producer.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)  # signal 0: check if process exists
        return True
    except (ValueError, ProcessLookupError, PermissionError, OSError):
        return False

tile1, tile2 = st.columns(2)

with tile1:
    st.subheader("Artist Touring Intensity")

    df_1 = load_df("touring_intensity", f"""
        SELECT artist_name, primary_genre, country, concert_count
        FROM `{_project}.{_dataset}.mart_artist_touring_intensity`
        ORDER BY concert_count DESC
        LIMIT 500
    """)
    if not df_1.empty:
        caption_text = (
            "Which artists are currently touring most intensively across US, CA, GB, DE, IT? "
            "What genres dominate?"
        )
        if date_subtitle:
            caption_text += f"  \n{date_subtitle}"
        st.caption(caption_text)

        top_artists = (
            df_1.groupby("artist_name", as_index=False)["concert_count"]
            .sum()
            .sort_values("concert_count", ascending=False)
            .head(15)
        )
        chart_df = (
            df_1[df_1["artist_name"].isin(top_artists["artist_name"])]
            .groupby(["artist_name", "country"], as_index=False)["concert_count"]
            .sum()
        )
        artist_order = (
            chart_df.groupby("artist_name", as_index=False)["concert_count"]
            .sum()
            .sort_values("concert_count", ascending=False)["artist_name"]
            .tolist()
        )
        artist_chart = (
            alt.Chart(chart_df)
            .mark_bar()
            .encode(
                x=alt.X(
                    "artist_name:N",
                    sort=artist_order,
                    title="Artist",
                    axis=alt.Axis(labelAngle=-90, labelLimit=100),
                ),
                y=alt.Y("concert_count:Q", title="Concert Count"),
                color=alt.Color("country:N", title="Country"),
            )
            .properties(height=350)
        )
        st.altair_chart(artist_chart, use_container_width=True)

        # Genre Intensity chart (below the artist chart)
        st.subheader("Genre Intensity")
        genre_caption = "Top genres by upcoming concert count"
        if date_subtitle:
            genre_caption += f"  \n{date_subtitle}"
        st.caption(genre_caption)

        genre_counts = (
            df_1[df_1["primary_genre"].notna()]
            .groupby("primary_genre", as_index=False)["concert_count"]
            .sum()
            .sort_values("concert_count", ascending=False)
            .head(10)
        )
        if not genre_counts.empty:
            genre_chart = (
                alt.Chart(genre_counts)
                .mark_bar()
                .encode(
                    x=alt.X("primary_genre:N", sort="-y", title="Genre"),
                    y=alt.Y("concert_count:Q", title="Concert Count"),
                )
                .properties(height=300)
            )
            st.altair_chart(genre_chart, use_container_width=True)
    else:
        st.info("No data yet in mart_artist_touring_intensity.")

    # ── Live Event Stream (inside Tile 1) ────────────────────────
    if df_live is not None and not df_live.empty:
        st.divider()

        stream_live = _is_stream_live()
        hdr_left, hdr_right = st.columns([3, 1])
        with hdr_left:
            st.subheader("Live Event Stream")
        with hdr_right:
            if stream_live:
                st.markdown(
                    '<div style="text-align:right; padding-top:0.6rem;">'
                    '<span style="background:#22c55e; color:white; padding:0.3rem 0.8rem; '
                    'border-radius:1rem; font-weight:600; font-size:0.85rem;">'
                    '\U0001f7e2 LIVE</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="text-align:right; padding-top:0.6rem;">'
                    '<span style="background:#6b7280; color:white; padding:0.3rem 0.8rem; '
                    'border-radius:1rem; font-weight:600; font-size:0.85rem;">'
                    '\u26aa OFFLINE</span></div>',
                    unsafe_allow_html=True,
                )
        refresh_note = "Polling every ~5 min. " if stream_live else "Currently offline. "
        st.caption(
            f"{refresh_note}Upcoming music events from Ticketmaster, "
            "streamed via Kafka and landed in BigQuery."
        )

        m1, m2, m3 = st.columns(3)
        total_events = df_live["event_id"].nunique()
        new_events = df_live[df_live["is_new"] == True]["event_id"].nunique()
        latest_ts = df_live["observed_at"].max()

        m1.metric("Events Tracked", f"{total_events:,}")
        m2.metric("New Listings", f"{new_events:,}")
        m3.metric(
            "Latest Update",
            latest_ts.strftime("%H:%M %d/%m/%Y") if pd.notna(latest_ts) else "\u2014",
        )

        live_left, live_right = st.columns(2)

        with live_left:
            st.caption("Event Status Breakdown")
            status_df = (
                df_live.groupby("event_status", as_index=False)
                .agg(count=("event_id", "nunique"))
                .sort_values("count", ascending=False)
            )
            status_chart = (
                alt.Chart(status_df)
                .mark_bar()
                .encode(
                    x=alt.X("count:Q", title="Unique Events"),
                    y=alt.Y("event_status:N", sort="-x", title="Status"),
                    color=alt.Color("event_status:N", legend=None),
                )
                .properties(height=max(len(status_df) * 35, 120))
            )
            st.altair_chart(status_chart, use_container_width=True)

        with live_right:
            st.caption("Recent Event Updates")
            recent = df_live.head(20)[
                ["artist_name", "event_name", "event_status", "city", "country_code", "observed_at"]
            ].copy()
            recent["observed_at"] = pd.to_datetime(recent["observed_at"]).dt.strftime("%H:%M %d/%m")
            st.dataframe(
                recent.rename(columns={
                    "artist_name": "Artist",
                    "event_name": "Event",
                    "event_status": "Status",
                    "city": "City",
                    "country_code": "Country",
                    "observed_at": "Observed",
                }),
                hide_index=True,
                use_container_width=True,
            )

# ── Tile 2: Setlist Repertoire + Freshness ──────────────────────────

with tile2:
    st.subheader("Setlist Evolution & Similarity")

    # Build a unified artist list from both datasets
    all_artists = sorted(
        set(df_repertoire["artist_name"].dropna().unique().tolist())
        | set(df_freshness["artist_name"].dropna().unique().tolist())
    ) if not df_repertoire.empty or not df_freshness.empty else []

    if all_artists:
        selected_artist = st.selectbox("Artist", all_artists, key="tile2_artist")
    else:
        selected_artist = None

    # ── Combined Repertoire + Freshness chart ──
    st.caption(
        "How many unique songs does each artist play per year of touring? (only recent setlist data)  \n"
        "The line shows the Freshness Index: % of songs appearing for the first time in the dataset.  \n"
        "(high = fresh repertoire, low = predictable setlist)"
    )

    if not df_repertoire.empty and selected_artist:
        filt = df_repertoire[
            (df_repertoire["artist_name"] == selected_artist)
            & (df_repertoire["concert_count"] >= MIN_SHOWS)
        ].sort_values("concert_year")

        s_filt = (
            df_freshness[
                (df_freshness["artist_name"] == selected_artist)
                & (df_freshness["concert_count"] >= MIN_SHOWS)
            ].sort_values("concert_year")
            if not df_freshness.empty
            else pd.DataFrame()
        )
        # Skip first year per artist (always 100 % fresh)
        if not s_filt.empty:
            s_filt = s_filt[s_filt["concert_year"] > s_filt["concert_year"].min()]

        if not filt.empty:
            genre = filt["primary_genre"].dropna().unique()
            if len(genre):
                st.caption(f"Genre: {genre[0]}")

            bars = (
                alt.Chart(filt)
                .mark_bar(color="#5276A7")
                .encode(
                    x=alt.X("concert_year:O", title="Year"),
                    y=alt.Y("unique_songs:Q", title="Unique Songs"),
                    tooltip=["concert_year", "concert_count", "unique_songs", "total_song_performances"],
                )
            )

            if not s_filt.empty:
                line = (
                    alt.Chart(s_filt)
                    .mark_line(color="#E45756", point=True, strokeWidth=2)
                    .encode(
                        x=alt.X("concert_year:O"),
                        y=alt.Y("freshness_pct:Q", title="Freshness %", scale=alt.Scale(domain=[0, 100])),
                        tooltip=["concert_year", "freshness_pct", "first_time_songs", "repeated_songs"],
                    )
                )
                combined = alt.layer(bars, line).resolve_scale(y="independent").properties(height=350)
            else:
                combined = bars.properties(height=350)

            st.altair_chart(combined, use_container_width=True)

            total_years = len(filt)
            total_unique = filt["unique_songs"].sum()
            total_perfs = filt["total_song_performances"].sum()
            st.caption(
                f"{total_years} years of data (\u2265{MIN_SHOWS} shows) \u2014 "
                f"{total_unique} unique songs total \u2014 "
                f"{total_perfs} total song performances"
            )

            if not s_filt.empty:
                st.dataframe(
                    s_filt[["concert_year", "concert_count", "unique_songs", "first_time_songs", "repeated_songs", "freshness_pct"]]
                    .rename(columns={
                        "concert_year": "Year",
                        "concert_count": "Shows",
                        "unique_songs": "Unique Songs",
                        "first_time_songs": "First-time Songs",
                        "repeated_songs": "Repeated",
                        "freshness_pct": "Freshness %",
                    }),
                    hide_index=True,
                )
        else:
            st.info(f"No years with \u2265{MIN_SHOWS} shows for {selected_artist}.")
    elif not selected_artist:
        st.info("No setlist data yet in mart_artist_yearly_repertoire.")

    # ── Spark: Artist Venue Similarity ────────────────────────────
    if df_similarity is not None and not df_similarity.empty and selected_artist:
        artist_sim = df_similarity[
            (df_similarity["artist_a"] == selected_artist)
            | (df_similarity["artist_b"] == selected_artist)
        ].copy()

        if not artist_sim.empty:
            artist_sim["similar_artist"] = artist_sim.apply(
                lambda r: r["artist_b"] if r["artist_a"] == selected_artist else r["artist_a"],
                axis=1,
            )
            artist_sim = artist_sim.sort_values("jaccard_similarity", ascending=False).head(10)

            st.subheader(f"Artists Playing at the Same Venues as {selected_artist}")
            st.caption(
                "Artists who have played at the same venues, "
                "ranked by Jaccard similarity.  \n"
                "(shared venues \u00f7 combined unique venues)"
            )

            sim_chart = (
                alt.Chart(artist_sim)
                .mark_bar()
                .encode(
                    x=alt.X("jaccard_similarity:Q", title="Jaccard Similarity"),
                    y=alt.Y("similar_artist:N", sort="-x", title="Artist"),
                    color=alt.Color(
                        "jaccard_similarity:Q",
                        scale=alt.Scale(scheme="tealblues"),
                        legend=None,
                    ),
                    tooltip=[
                        alt.Tooltip("similar_artist:N", title="Artist"),
                        alt.Tooltip("shared_venues:Q", title="Shared Venues"),
                        alt.Tooltip("jaccard_similarity:Q", title="Jaccard Score", format=".4f"),
                    ],
                )
                .properties(height=max(len(artist_sim) * 35 + 40, 120))
            )
            st.altair_chart(sim_chart, use_container_width=True)

            st.dataframe(
                artist_sim[["similar_artist", "shared_venues", "jaccard_similarity"]]
                .rename(columns={
                    "similar_artist": "Artist",
                    "shared_venues": "Shared Venues",
                    "jaccard_similarity": "Jaccard Score",
                }),
                hide_index=True,
            )
