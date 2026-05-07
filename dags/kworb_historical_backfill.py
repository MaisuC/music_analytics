from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import json
import requests
from bs4 import BeautifulSoup


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@task
def extract_kworb():
    """
    Fetch Kworb pages for configured markets.
    """
    kworb_config = Variable.get("kworb_config", deserialize_json=True)

    markets = kworb_config["markets"]
    chart_type = kworb_config["chart_type"]
    base_url = kworb_config["base_url"]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }

    payloads = {}

    for market in markets:
        url = f"{base_url}/{market}_{chart_type}.html"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        payloads[market] = {
            "url": url,
            "html": response.text
        }

    return payloads


@task
def transform_kworb(raw_payloads: dict):
    """
    Parse Kworb table and normalize records.
    Expected headers:
    Pos, P+, Artist and Title, Days, Pk, (x?), Streams, Streams+, 7Day, 7Day+, Total
    """
    kworb_config = Variable.get("kworb_config", deserialize_json=True)
    chart_type = kworb_config["chart_type"]

    utc_now = datetime.utcnow()
    ingested_at = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    ingested_date = utc_now.strftime("%Y-%m-%d")

    all_records = []

    for market, payload in raw_payloads.items():
        url = payload["url"]
        html = payload["html"]

        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")

        if table is None:
            raise ValueError(f"No table found for market={market}, url={url}")

        rows = table.find_all("tr")
        if not rows:
            raise ValueError(f"No rows found for market={market}, url={url}")

        header_cells = rows[0].find_all(["th", "td"])
        headers = [cell.get_text(" ", strip=True).lower() for cell in header_cells]

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue

            values = [cell.get_text(" ", strip=True) for cell in cells]

            if len(values) < len(headers):
                values.extend([None] * (len(headers) - len(values)))

            record_map = dict(zip(headers, values))

            first_link = row.find("a", href=True)
            row_link = first_link["href"] if first_link else None

            rank = record_map.get("pos")
            rank_change = record_map.get("p+")
            artist_and_title = record_map.get("artist and title")
            days_on_chart = record_map.get("days")
            peak_rank = record_map.get("pk")
            multiplier = record_map.get("(x?)")
            streams = record_map.get("streams")
            streams_change = record_map.get("streams+")
            seven_day = record_map.get("7day")
            seven_day_change = record_map.get("7day+")
            total_streams = record_map.get("total")

            artist_name = None
            track_name = None

            if artist_and_title:
                for sep in [" - ", " – ", " — "]:
                    if sep in artist_and_title:
                        artist_name, track_name = artist_and_title.split(sep, 1)
                        break

                if track_name is None:
                    track_name = artist_and_title

            all_records.append({
                "market": market,
                "chart_type": chart_type,
                "rank": rank,
                "rank_change": rank_change,
                "artist_and_title": artist_and_title,
                "artist_name": artist_name,
                "track_name": track_name,
                "days_on_chart": days_on_chart,
                "peak_rank": peak_rank,
                "multiplier": multiplier,
                "streams": streams,
                "streams_change": streams_change,
                "seven_day": seven_day,
                "seven_day_change": seven_day_change,
                "total_streams": total_streams,
                "source_url": url,
                "row_link": row_link,
                "ingested_date": ingested_date,
                "ingested_at": ingested_at,
                "raw_row_json": json.dumps(record_map),
            })

    if not all_records:
        raise ValueError("No Kworb records were transformed.")

    return all_records


@task
def load_to_snowflake(records: list[dict]):
    """
    Idempotent daily load:
    delete today's partition for each market/chart_type, then insert.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_music_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    target_table = "MUSIC_ANALYTICS_DB.RAW.RAW_KWORB_CHARTS"

    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                market              STRING,
                chart_type          STRING,
                rank                STRING,
                rank_change         STRING,
                artist_and_title    STRING,
                artist_name         STRING,
                track_name          STRING,
                days_on_chart       STRING,
                peak_rank           STRING,
                multiplier          STRING,
                streams             STRING,
                streams_change      STRING,
                seven_day           STRING,
                seven_day_change    STRING,
                total_streams       STRING,
                source_url          STRING,
                row_link            STRING,
                ingested_date       DATE,
                ingested_at         TIMESTAMP_NTZ,
                raw_row_json        STRING
            )
        """)

        partitions = sorted({
            (r["market"], r["chart_type"], r["ingested_date"])
            for r in records
        })

        delete_sql = f"""
            DELETE FROM {target_table}
            WHERE market = %s
              AND chart_type = %s
              AND ingested_date = %s
        """

        for market, chart_type, ingested_date in partitions:
            cur.execute(delete_sql, (market, chart_type, ingested_date))

        insert_sql = f"""
            INSERT INTO {target_table}
            (
                market,
                chart_type,
                rank,
                rank_change,
                artist_and_title,
                artist_name,
                track_name,
                days_on_chart,
                peak_rank,
                multiplier,
                streams,
                streams_change,
                seven_day,
                seven_day_change,
                total_streams,
                source_url,
                row_link,
                ingested_date,
                ingested_at,
                raw_row_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                r["market"],
                r["chart_type"],
                r["rank"],
                r["rank_change"],
                r["artist_and_title"],
                r["artist_name"],
                r["track_name"],
                r["days_on_chart"],
                r["peak_rank"],
                r["multiplier"],
                r["streams"],
                r["streams_change"],
                r["seven_day"],
                r["seven_day_change"],
                r["total_streams"],
                r["source_url"],
                r["row_link"],
                r["ingested_date"],
                r["ingested_at"],
                r["raw_row_json"],
            )
            for r in records
        ]

        cur.executemany(insert_sql, data)

        cur.execute("COMMIT;")
        print(f"Inserted {len(records)} records into {target_table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="kworb_historical_backfill",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    schedule=None,
    tags=["kworb", "historical", "raw"],
) as dag:

    raw_pages = extract_kworb()
    records = transform_kworb(raw_pages)
    load_to_snowflake(records)