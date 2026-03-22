/* @bruin
name: staging.stg_concerts_union
type: bq.sql
connection: bigquery
depends:
  - orchestration.run_dlt_ingestion
materialization:
  type: view
@bruin */

SELECT
  e.event_id AS concert_id,
  m.mbid AS artist_mbid,
  e.venue_id,
  DATE(e.event_date) AS event_date,
  e.country_code AS country,
  CAST(NULL AS FLOAT64) AS ticket_price_min_gbp,
  CAST(NULL AS FLOAT64) AS ticket_price_max_gbp,
  e.event_status,
  CURRENT_TIMESTAMP() AS ingested_at
FROM raw.ticketmaster_events e
LEFT JOIN raw.musicbrainz_artists m
  ON LOWER(TRIM(e.artist_name)) = LOWER(TRIM(m.artist_name))
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY e.event_id
  ORDER BY m.extracted_at DESC NULLS LAST
) = 1
