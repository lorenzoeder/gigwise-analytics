import os

import altair as alt
import pandas as pd
import streamlit as st
from google.cloud import bigquery

st.set_page_config(page_title="GigWise Analytics", layout="wide")
st.title("GigWise Analytics Dashboard")
st.caption("Concert touring intensity and setlist evolution")

project_id = os.getenv("GCP_PROJECT_ID")
dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

if not project_id:
    st.error("Set GCP_PROJECT_ID in your environment before running the dashboard.")
    st.stop()

client = bigquery.Client(project=project_id)

@st.cache_data(ttl=1800)
def run_query(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()

# ── Shared date range for Tile 1 ─────────────────────────────────────
date_range = run_query(f"""
    SELECT MIN(event_date) AS min_date, MAX(event_date) AS max_date
    FROM `{project_id}.{dataset}.fact_concert`
    WHERE source = 'ticketmaster'
""")
date_subtitle = ""
if not date_range.empty and date_range.iloc[0]["min_date"] is not None:
    d_min = date_range.iloc[0]["min_date"].strftime("%d/%m/%Y")
    d_max = date_range.iloc[0]["max_date"].strftime("%d/%m/%Y")
    date_subtitle = f"Upcoming concerts: {d_min} \u2013 {d_max}"

# ── Tile 1: Artist Touring Intensity ─────────────────────────────────
# Two charts stacked vertically: top = artist bar, bottom = genre bar

tile1, tile2 = st.columns(2)

with tile1:
    st.subheader("Artist Touring Intensity")

    df_1 = run_query(f"""
        SELECT artist_name, primary_genre, country, concert_count
        FROM `{project_id}.{dataset}.mart_artist_touring_intensity`
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

# ── Tile 2: Setlist Repertoire + Freshness ──────────────────────────

df_repertoire = run_query(f"""
    SELECT artist_name, primary_genre, concert_year, concert_count,
           unique_songs, total_song_performances
    FROM `{project_id}.{dataset}.mart_artist_yearly_repertoire`
    WHERE artist_name IS NOT NULL
    ORDER BY artist_name, concert_year
    LIMIT 5000
""")

df_freshness = run_query(f"""
    SELECT artist_name, primary_genre, concert_year, concert_count,
           unique_songs, first_time_songs, repeated_songs, freshness_pct
    FROM `{project_id}.{dataset}.mart_artist_setlist_freshness`
    WHERE artist_name IS NOT NULL
    ORDER BY artist_name, concert_year
    LIMIT 5000
""")

MIN_SHOWS = 5

with tile2:
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
    st.subheader("Setlist Repertoire Over Time")
    st.caption(
        "How many unique songs does each artist play per year of touring? (data from 2000 onward)  \n"
        "The line shows the Freshness Index: % of songs appearing for the first time in the dataset "
        "(high = fresh repertoire, low = predictable setlist)."
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
