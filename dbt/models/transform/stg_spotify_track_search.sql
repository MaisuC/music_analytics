select
    query_term,
    track_id,
    trim(track_name) as track_name,
    artist_id,
    trim(artist_name) as artist_name,
    popularity::number as popularity,
    snapshot_date::date as snapshot_date,
    fetched_at::timestamp_ntz as fetched_at,
    raw_payload
from {{ source('raw', 'raw_spotify_track_search') }}