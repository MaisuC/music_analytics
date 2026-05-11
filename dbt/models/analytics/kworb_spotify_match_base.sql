with spotify_latest as (
    select
        query_term,
        track_id,
        track_name,
        artist_id,
        artist_name,
        popularity,
        snapshot_date,
        fetched_at
    from {{ ref('spotify_latest_tracks') }}
),

kworb_latest as (
    select
        market,
        chart_type,
        rank,
        rank_change,
        artist_name,
        track_name,
        days_on_chart,
        peak_rank,
        streams,
        seven_day,
        total_streams,
        ingested_date,
        ingested_at
    from {{ ref('stg_kworb_charts') }}
)

select
    k.market,
    k.chart_type,
    k.rank,
    k.rank_change,
    k.artist_name as kworb_artist_name,
    k.track_name as kworb_track_name,
    k.days_on_chart,
    k.peak_rank,
    k.streams,
    k.seven_day,
    k.total_streams,
    k.ingested_date,
    k.ingested_at,

    s.query_term,
    s.track_id as spotify_track_id,
    s.artist_id as spotify_artist_id,
    s.artist_name as spotify_artist_name,
    s.track_name as spotify_track_name,
    s.popularity,
    s.snapshot_date,
    s.fetched_at

from kworb_latest k
left join spotify_latest s
    on upper(trim(k.artist_name)) = upper(trim(s.artist_name))
   and upper(trim(k.track_name)) = upper(trim(s.track_name))