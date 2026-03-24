SELECT
  CAST(setlist_id AS STRING) AS setlist_id,
  CAST(artist_mbid AS STRING) AS artist_mbid,
  CAST(artist_name AS STRING) AS artist_name,
  COALESCE(
    SAFE.PARSE_DATE('%d-%m-%Y', event_date),
    SAFE_CAST(event_date AS DATE)
  ) AS event_date,
  CAST(country_code AS STRING) AS country,
  CAST(city AS STRING) AS city,
  CAST(venue_name AS STRING) AS venue_name,
  CAST(song_count AS INT64) AS songs_played_count,
  CAST(songs_played AS STRING) AS songs_played,
  CAST(tour_name AS STRING) AS tour_id,
  CAST(extracted_at AS TIMESTAMP) AS extracted_at
FROM {{ source('raw', 'setlistfm_setlists') }}
WHERE COALESCE(
  SAFE.PARSE_DATE('%d-%m-%Y', event_date),
  SAFE_CAST(event_date AS DATE)
) >= DATE('2000-01-01')
