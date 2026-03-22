SELECT
  CAST(setlist_id AS STRING) AS setlist_id,
  CAST(artist_mbid AS STRING) AS artist_mbid,
  COALESCE(
    SAFE.PARSE_DATE('%d-%m-%Y', event_date),
    SAFE_CAST(event_date AS DATE)
  ) AS event_date,
  CAST(song_count AS INT64) AS songs_played_count,
  FALSE AS had_encore,
  CAST(tour_name AS STRING) AS tour_id
FROM {{ source('raw', 'setlistfm_setlists') }}
