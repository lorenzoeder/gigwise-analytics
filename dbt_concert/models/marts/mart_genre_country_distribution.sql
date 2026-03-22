SELECT
  d.primary_genre,
  f.country,
  EXTRACT(YEAR FROM f.event_date) AS event_year,
  COUNT(*) AS concert_count
FROM {{ ref('fact_concert') }} f
LEFT JOIN {{ ref('dim_artist') }} d
  ON f.artist_id = d.artist_id
GROUP BY 1, 2, 3
