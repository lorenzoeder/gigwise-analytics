SELECT
  concert_id,
  artist_mbid,
  artist_name,
  event_date,
  country,
  city,
  venue_name,
  event_status,
  CAST(NULL AS STRING) AS setlist_id,
  CAST(NULL AS INT64) AS songs_played_count,
  CAST(NULL AS STRING) AS songs_played,
  CAST(NULL AS STRING) AS tour_id,
  'ticketmaster' AS source,
  extracted_at
FROM {{ ref('stg_ticketmaster__events') }}

UNION ALL

SELECT
  setlist_id AS concert_id,
  artist_mbid,
  artist_name,
  event_date,
  country,
  city,
  venue_name,
  CAST(NULL AS STRING) AS event_status,
  setlist_id,
  songs_played_count,
  songs_played,
  tour_id,
  'setlistfm' AS source,
  extracted_at
FROM {{ ref('stg_setlistfm__setlists') }}
