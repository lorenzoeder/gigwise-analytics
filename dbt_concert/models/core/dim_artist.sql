SELECT
  artist_id,
  mbid,
  artist_name,
  primary_genre
FROM {{ ref('stg_spotify__artists') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY snapshot_date DESC) = 1
