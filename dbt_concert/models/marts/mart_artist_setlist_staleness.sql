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

with_prior AS (
  SELECT
    curr.artist_name,
    curr.primary_genre,
    curr.concert_year,
    COUNT(*) AS unique_songs,
    COUNTIF(prev.song_name IS NOT NULL) AS songs_from_prior_year
  FROM distinct_songs curr
  LEFT JOIN distinct_songs prev
    ON curr.artist_name = prev.artist_name
    AND curr.song_name = prev.song_name
    AND prev.concert_year = curr.concert_year - 1
  GROUP BY 1, 2, 3
)

SELECT
  w.artist_name,
  w.primary_genre,
  w.concert_year,
  c.concert_count,
  w.unique_songs,
  w.songs_from_prior_year,
  w.unique_songs - w.songs_from_prior_year AS new_songs,
  ROUND(
    SAFE_DIVIDE(w.songs_from_prior_year, w.unique_songs) * 100,
    1
  ) AS staleness_pct
FROM with_prior w
JOIN concert_counts c
  ON w.artist_name = c.artist_name
  AND w.concert_year = c.concert_year
