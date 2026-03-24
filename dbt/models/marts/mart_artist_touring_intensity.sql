SELECT
  COALESCE(d.artist_name, f.artist_name) AS artist_name,
  INITCAP(d.primary_genre) AS primary_genre,
  f.country,
  COUNT(*) AS concert_count
FROM {{ ref('fact_concert') }} f
LEFT JOIN {{ ref('dim_artist') }} d
  ON f.artist_id = d.artist_id
WHERE f.source = 'ticketmaster'
  AND f.artist_id IS NOT NULL
  AND f.artist_name IS NOT NULL
GROUP BY 1, 2, 3
