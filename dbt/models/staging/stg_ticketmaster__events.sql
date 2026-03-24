SELECT
  CAST(e.event_id AS STRING) AS concert_id,
  CAST(COALESCE(e.artist_mbid, m.mbid) AS STRING) AS artist_mbid,
  CAST(e.artist_name AS STRING) AS artist_name,
  CAST(e.venue_id AS STRING) AS venue_id,
  CAST(e.venue_name AS STRING) AS venue_name,
  CAST(e.city AS STRING) AS city,
  DATE(e.event_date) AS event_date,
  CAST(e.country_code AS STRING) AS country,
  CAST(e.event_status AS STRING) AS event_status
FROM {{ source('raw', 'ticketmaster_events') }} e
LEFT JOIN {{ source('raw', 'musicbrainz_artists') }} m
  ON LOWER(TRIM(e.artist_name)) = LOWER(TRIM(m.artist_name))
WHERE COALESCE(e.event_status, '') NOT IN ('cancelled', 'postponed')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.event_id
  ORDER BY (e.artist_mbid IS NULL), m.extracted_at DESC NULLS LAST
) = 1
