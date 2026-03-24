WITH concerts AS (
  SELECT
    COALESCE(d.artist_name, f.artist_name) AS artist_name,
    INITCAP(d.primary_genre) AS primary_genre,
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
    concert_year,
    COUNT(DISTINCT concert_id) AS concert_count
  FROM concerts
  GROUP BY 1, 2
),

yearly_songs AS (
  SELECT
    artist_name,
    primary_genre,
    concert_year,
    TRIM(song) AS song_name
  FROM concerts
  CROSS JOIN UNNEST(SPLIT(songs_played, '; ')) AS song
  WHERE songs_played IS NOT NULL
),

distinct_songs AS (
  SELECT DISTINCT
    artist_name,
    primary_genre,
    concert_year,
    song_name
  FROM yearly_songs
  WHERE song_name != ''
),

-- For each song+artist, find the earliest year it appeared
first_appearance AS (
  SELECT
    artist_name,
    song_name,
    MIN(concert_year) AS first_year
  FROM distinct_songs
  GROUP BY 1, 2
),

with_freshness AS (
  SELECT
    ds.artist_name,
    ds.primary_genre,
    ds.concert_year,
    COUNT(*) AS unique_songs,
    COUNTIF(fa.first_year = ds.concert_year) AS first_time_songs,
    COUNTIF(fa.first_year < ds.concert_year) AS repeated_songs
  FROM distinct_songs ds
  JOIN first_appearance fa
    ON ds.artist_name = fa.artist_name
    AND ds.song_name = fa.song_name
  GROUP BY 1, 2, 3
)

SELECT
  w.artist_name,
  w.primary_genre,
  w.concert_year,
  c.concert_count,
  w.unique_songs,
  w.first_time_songs,
  w.repeated_songs,
  ROUND(
    SAFE_DIVIDE(w.first_time_songs, w.unique_songs) * 100,
    1
  ) AS freshness_pct
FROM with_freshness w
JOIN concert_counts c
  ON w.artist_name = c.artist_name
  AND w.concert_year = c.concert_year
