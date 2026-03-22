SELECT
  CAST(e.event_id AS STRING) AS concert_id,
  CAST(m.mbid AS STRING) AS artist_mbid,
  CAST(e.venue_id AS STRING) AS venue_id,
  DATE(e.event_date) AS event_date,
  CAST(e.country_code AS STRING) AS country,
  CAST(NULL AS FLOAT64) AS ticket_price_min_gbp,
  CAST(NULL AS FLOAT64) AS ticket_price_max_gbp,
  CAST(e.event_status AS STRING) AS event_status,
  CURRENT_TIMESTAMP() AS ingested_at
FROM {{ source('raw', 'ticketmaster_events') }} e
LEFT JOIN {{ source('raw', 'musicbrainz_artists') }} m
  ON LOWER(TRIM(e.artist_name)) = LOWER(TRIM(m.artist_name))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.event_id
  ORDER BY m.extracted_at DESC NULLS LAST
) = 1
