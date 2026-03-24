SELECT
  COALESCE(d.artist_name, f.artist_name) AS artist_name,
  d.primary_genre,
  f.event_date,
  f.songs_played_count,
  f.source,
  f.city,
  f.venue_name
FROM {{ ref('fact_concert') }} f
LEFT JOIN {{ ref('dim_artist') }} d
  ON f.artist_id = d.artist_id
