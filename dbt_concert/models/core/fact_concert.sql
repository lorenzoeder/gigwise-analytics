SELECT
  c.concert_id,
  a.artist_id,
  c.venue_id,
  c.tour_id,
  c.event_date,
  c.country,
  c.ticket_price_min_gbp,
  c.ticket_price_max_gbp,
  c.event_status,
  c.setlist_id,
  c.songs_played_count,
  c.had_encore,
  CURRENT_TIMESTAMP() AS loaded_at
FROM {{ ref('int_concerts_unified') }} c
LEFT JOIN {{ ref('stg_spotify__artists') }} a
  ON c.artist_mbid = a.mbid
