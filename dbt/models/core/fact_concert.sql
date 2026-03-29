{{
  config(
    materialized='incremental',
    unique_key='concert_id',
    on_schema_change='sync_all_columns',
    partition_by={
      'field': 'event_date',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['artist_id']
  )
}}

SELECT
  c.concert_id,
  d.artist_id,
  COALESCE(d.artist_name, c.artist_name) AS artist_name,
  c.event_date,
  c.country,
  c.city,
  c.venue_name,
  c.event_status,
  c.setlist_id,
  c.songs_played_count,
  c.songs_played,
  c.tour_id,
  c.source,
  CURRENT_TIMESTAMP() AS loaded_at
FROM {{ ref('int_concerts_unified') }} c
LEFT JOIN {{ ref('dim_artist') }} d
  ON c.artist_mbid = d.mbid
{% if is_incremental() %}
WHERE c.extracted_at > (SELECT MAX(loaded_at) FROM {{ this }})
{% endif %}
