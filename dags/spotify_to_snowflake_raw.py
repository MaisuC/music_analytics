from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import base64
import json
import requests


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


@task
def extract_spotify():
    spotify_config = Variable.get("spotify_config", deserialize_json=True)

    client_id = spotify_config["client_id"]
    client_secret = spotify_config["client_secret"]
    token_url = spotify_config["token_url"]
    base_url = spotify_config["base_url"]
    queries = spotify_config["queries"]
    search_type = spotify_config["type"]
    limit = spotify_config["limit"]
    offsets = spotify_config.get("offsets", [0])

    auth_str = f"{client_id}:{client_secret}"
    b64_auth = base64.b64encode(auth_str.encode()).decode()

    token_response = requests.post(
        token_url,
        headers={"Authorization": f"Basic {b64_auth}"},
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    token_response.raise_for_status()

    token = token_response.json().get("access_token")
    if not token:
        raise ValueError("Spotify token not received.")

    all_items = []

    for query in queries:
        for offset in offsets:
            response = requests.get(
                f"{base_url}/search",
                headers={"Authorization": f"Bearer {token}"},
                params={
                    "q": query,
                    "type": search_type,
                    "limit": limit,
                    "offset": offset,
                },
                timeout=30,
            )
            response.raise_for_status()

            payload = response.json()
            items = payload.get("tracks", {}).get("items", [])

            for item in items:
                item["_query_term"] = query

            all_items.extend(items)

    if not all_items:
        raise ValueError("No Spotify items were returned.")

    return {"tracks": {"items": all_items}}


@task
def transform_spotify(raw_data: dict):
    utc_now = datetime.utcnow()
    fetched_at = utc_now.strftime("%Y-%m-%d %H:%M:%S")
    snapshot_date = utc_now.strftime("%Y-%m-%d")

    items = raw_data.get("tracks", {}).get("items", [])
    deduped = {}
    for item in items:
        track_id = item.get("id")
        if track_id and track_id not in deduped:
            deduped[track_id] = item

    records = []
    for item in deduped.values():
        artists = item.get("artists", [])
        first_artist = artists[0] if artists else {}

        records.append({
            "query_term": item.get("_query_term"),
            "track_id": item.get("id"),
            "track_name": item.get("name"),
            "artist_id": first_artist.get("id"),
            "artist_name": first_artist.get("name"),
            "popularity": item.get("popularity"),
            "snapshot_date": snapshot_date,
            "fetched_at": fetched_at,
            "raw_payload": json.dumps(item),
        })

    if not records:
        raise ValueError("No Spotify track records were transformed.")

    return records



@task
def load_to_snowflake(records: list[dict]):
    """
    Idempotent daily load for multi-query Spotify ingestion.
    For each (query_term, snapshot_date) partition:
      - delete existing rows
      - insert fresh rows
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_music_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    target_table = "MUSIC_ANALYTICS_DB.RAW.RAW_SPOTIFY_TRACK_SEARCH"

    try:
        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                query_term      STRING,
                track_id        STRING,
                track_name      STRING,
                artist_id       STRING,
                artist_name     STRING,
                popularity      NUMBER,
                snapshot_date   DATE,
                fetched_at      TIMESTAMP_NTZ,
                raw_payload     STRING
            )
        """)

        # delete existing partitions for this batch
        partitions = sorted({
            (record["query_term"], record["snapshot_date"])
            for record in records
        })

        delete_sql = f"""
            DELETE FROM {target_table}
            WHERE query_term = %s
              AND snapshot_date = %s
        """

        for query_term, snapshot_date in partitions:
            cur.execute(delete_sql, (query_term, snapshot_date))

        insert_sql = f"""
            INSERT INTO {target_table}
            (
                query_term,
                track_id,
                track_name,
                artist_id,
                artist_name,
                popularity,
                snapshot_date,
                fetched_at,
                raw_payload
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                record["query_term"],
                record["track_id"],
                record["track_name"],
                record["artist_id"],
                record["artist_name"],
                record["popularity"],
                record["snapshot_date"],
                record["fetched_at"],
                record["raw_payload"],
            )
            for record in records
        ]

        cur.executemany(insert_sql, data)

        cur.execute("COMMIT;")
        print(
            f"Inserted {len(records)} records into {target_table} "
            f"for {len(partitions)} partition(s)"
        )

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Load failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="spotify_to_snowflake_raw",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    catchup=False,
    schedule="0 4 * * *",
    tags=["spotify", "etl", "raw"],
) as dag:

    raw_data = extract_spotify()
    records = transform_spotify(raw_data)
    load_to_snowflake(records)