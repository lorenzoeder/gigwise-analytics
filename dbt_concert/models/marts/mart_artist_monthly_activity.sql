SELECT
  f.artist_id,
  d.artist_name,
  DATE_TRUNC(f.event_date, MONTH) AS event_month,
  COUNT(*) AS shows_count,
  AVG(s.spotify_popularity) AS avg_spotify_popularity
FROM {{ ref('fact_concert') }} f
LEFT JOIN {{ ref('dim_artist') }} d
  ON f.artist_id = d.artist_id
LEFT JOIN {{ ref('fact_artist_snapshot') }} s
  ON f.artist_id = s.artist_id
 AND f.event_date = s.snapshot_date
GROUP BY 1, 2, 3
