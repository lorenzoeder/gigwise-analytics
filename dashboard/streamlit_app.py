import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery

st.set_page_config(page_title="GigWise Analytics", layout="wide")
st.title("GigWise Analytics Dashboard")
st.caption("Concert tours, setlists, and artist popularity")

project_id = os.getenv("GCP_PROJECT_ID")
dataset = os.getenv("BQ_DATASET_ANALYTICS", "analytics")

if not project_id:
    st.error("Set GCP_PROJECT_ID in your environment before running the dashboard.")
    st.stop()

client = bigquery.Client(project=project_id)

@st.cache_data(ttl=1800)
def run_query(query: str) -> pd.DataFrame:
    return client.query(query).to_dataframe()

left, right = st.columns(2)

with left:
    st.subheader("Tile 1: Genre and Country Distribution")
    query_tile_1 = f"""
        SELECT primary_genre, country, event_year, concert_count
        FROM `{project_id}.{dataset}.mart_genre_country_distribution`
        ORDER BY concert_count DESC
        LIMIT 1000
    """
    df_1 = run_query(query_tile_1)
    if not df_1.empty:
        st.bar_chart(df_1, x="country", y="concert_count", color="primary_genre")
    else:
        st.info("No data yet in mart_genre_country_distribution.")

with right:
    st.subheader("Tile 2: Artist Touring Activity Over Time")
    query_tile_2 = f"""
        SELECT artist_name, event_month, shows_count, avg_spotify_popularity
        FROM `{project_id}.{dataset}.mart_artist_monthly_activity`
        ORDER BY event_month DESC
        LIMIT 2000
    """
    df_2 = run_query(query_tile_2)
    if not df_2.empty:
        artist_options = sorted(df_2["artist_name"].dropna().unique().tolist())
        selected_artist = st.selectbox("Artist", artist_options)
        filtered = df_2[df_2["artist_name"] == selected_artist]
        st.line_chart(filtered.set_index("event_month")[["shows_count", "avg_spotify_popularity"]])
    else:
        st.info("No data yet in mart_artist_monthly_activity.")
