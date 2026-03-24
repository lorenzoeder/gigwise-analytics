{{
  config(
    materialized='incremental',
    unique_key='artist_id',
    on_schema_change='sync_all_columns'
  )
}}

WITH musicbrainz_latest AS (
  SELECT
    CAST(mbid AS STRING) AS mbid,
    CAST(artist_name AS STRING) AS artist_name,
    CAST(primary_genre AS STRING) AS primary_genre,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at
  FROM {{ source('raw', 'musicbrainz_artists') }}
  WHERE mbid IS NOT NULL
    AND COALESCE(artist_type, 'Person') IN ('Person', 'Group', 'Orchestra', 'Choir')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY mbid
    ORDER BY extracted_at DESC
  ) = 1
),
ticketmaster_fallback AS (
  SELECT
    artist_mbid AS mbid,
    artist_name,
    CAST(extracted_at AS TIMESTAMP) AS extracted_at
  FROM {{ source('raw', 'ticketmaster_events') }}
  WHERE artist_mbid IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY artist_mbid
    ORDER BY extracted_at DESC
  ) = 1
),
combined AS (
  SELECT
    mbid,
    artist_name,
    primary_genre,
    extracted_at
  FROM musicbrainz_latest

  UNION ALL

  SELECT
    t.mbid,
    t.artist_name,
    CAST(NULL AS STRING) AS primary_genre,
    t.extracted_at
  FROM ticketmaster_fallback t
  LEFT JOIN musicbrainz_latest m ON t.mbid = m.mbid
  WHERE m.mbid IS NULL
)
SELECT
  mbid AS artist_id,
  mbid,
  artist_name,
  primary_genre,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM combined
{% if is_incremental() %}
WHERE extracted_at > (SELECT COALESCE(MAX(_loaded_at), TIMESTAMP('1970-01-01')) FROM {{ this }})
{% endif %}
