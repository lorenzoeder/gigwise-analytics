SELECT
  e.concert_id,
  e.artist_mbid,
  e.venue_id,
  e.event_date,
  e.country,
  e.ticket_price_min_gbp,
  e.ticket_price_max_gbp,
  e.event_status,
  s.setlist_id,
  s.songs_played_count,
  s.had_encore,
  s.tour_id
FROM {{ ref('stg_ticketmaster__events') }} e
LEFT JOIN {{ ref('stg_setlistfm__setlists') }} s
  ON e.artist_mbid = s.artist_mbid
 AND e.event_date = s.event_date
