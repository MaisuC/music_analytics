with latest_snapshot as (
    select max(snapshot_date) as snapshot_date
    from {{ ref('stg_spotify_track_search') }}
),

latest_rows as (
    select
        s.query_term,
        s.track_id,
        s.track_name,
        s.artist_id,
        s.artist_name,
        s.popularity,
        s.snapshot_date,
        s.fetched_at,
        row_number() over (
            partition by s.track_id
            order by s.popularity desc, s.fetched_at desc
        ) as rn
    from {{ ref('stg_spotify_track_search') }} s
    join latest_snapshot l
        on s.snapshot_date = l.snapshot_date
)

select
    query_term,
    track_id,
    track_name,
    artist_id,
    artist_name,
    popularity,
    snapshot_date,
    fetched_at
from latest_rows
where rn = 1