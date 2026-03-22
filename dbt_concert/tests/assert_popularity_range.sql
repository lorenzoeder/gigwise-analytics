SELECT *
FROM {{ ref('fact_artist_snapshot') }}
WHERE spotify_popularity < 0 OR spotify_popularity > 100
