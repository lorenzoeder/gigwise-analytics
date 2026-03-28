-- Ensure no artist appears with an invalid MusicBrainz artist type
-- (e.g. festivals, brands) that slipped through ingestion filters.
SELECT *
FROM {{ ref('dim_artist') }} d
JOIN {{ source('raw', 'musicbrainz_artists') }} m
  ON d.mbid = m.mbid
WHERE m.artist_type IS NOT NULL
  AND m.artist_type NOT IN ('Person', 'Group', 'Orchestra', 'Choir')
