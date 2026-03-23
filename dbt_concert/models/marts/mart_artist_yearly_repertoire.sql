WITH concerts AS (
  SELECT
    COALESCE(d.artist_name, f.artist_name) AS artist_name,
    d.primary_genre,
    EXTRACT(YEAR FROM f.event_date) AS concert_year,
    f.concert_id,
    f.songs_played
  FROM {{ ref('fact_concert') }} f
  LEFT JOIN {{ ref('dim_artist') }} d
    ON f.artist_id = d.artist_id
  WHERE f.source = 'setlistfm'
    AND f.event_date IS NOT NULL
),

concert_counts AS (
  SELECT
    artist_name,
    primary_genre,
    concert_year,
    COUNT(DISTINCT concert_id) AS concert_count
  FROM concerts
  GROUP BY 1, 2, 3
),

setlist_songs AS (
  SELECT
    artist_name,
    primary_genre,
    concert_year,
    TRIM(song) AS song_name
  FROM concerts
  CROSS JOIN UNNEST(SPLIT(songs_played, '; ')) AS song
  WHERE songs_played IS NOT NULL
)
SELECT
  s.artist_name,
  s.primary_genre,
  s.concert_year,
  c.concert_count,
  COUNT(DISTINCT s.song_name) AS unique_songs,
  COUNT(*) AS total_song_performances
FROM setlist_songs s
JOIN concert_counts c
  ON s.artist_name = c.artist_name
  AND s.concert_year = c.concert_year
WHERE s.song_name != ''
GROUP BY 1, 2, 3, 4
