select
    market,
    chart_type,
    rank,
    kworb_artist_name,
    kworb_track_name,
    query_term,
    spotify_artist_name,
    spotify_track_name,
    spotify_track_id,
    spotify_artist_id,
    popularity as spotify_popularity,
    streams,
    seven_day,
    total_streams,
    ingested_date,
    snapshot_date
from {{ ref('kworb_spotify_match_base') }}
where spotify_track_id is not null