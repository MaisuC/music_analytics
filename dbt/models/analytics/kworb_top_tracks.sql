select
    market,
    chart_type,
    rank,
    artist_name,
    track_name,
    streams,
    seven_day,
    total_streams,
    ingested_date
from {{ ref('stg_kworb_charts') }}
where rank <= 100