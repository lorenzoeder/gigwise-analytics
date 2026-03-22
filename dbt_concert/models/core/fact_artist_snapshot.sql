SELECT
  CONCAT(artist_id, '-', CAST(snapshot_date AS STRING)) AS snapshot_id,
  artist_id,
  snapshot_date,
  spotify_popularity,
  spotify_followers
FROM {{ ref('stg_spotify__artists') }}
