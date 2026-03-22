SELECT
  CAST(artist_id AS STRING) AS artist_id,
  CAST(mbid AS STRING) AS mbid,
  CAST(name AS STRING) AS artist_name,
  CAST(primary_genre AS STRING) AS primary_genre,
  CAST(popularity AS INT64) AS spotify_popularity,
  CAST(followers AS INT64) AS spotify_followers,
  DATE(snapshot_date) AS snapshot_date
FROM {{ source('raw', 'spotify_artists') }}
