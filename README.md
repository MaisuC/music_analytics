# Global Music Popularity Analytics Platform

An end-to-end cloud-based data engineering pipeline designed to ingest, transform, and visualize global music trends. This platform integrates historical chart data with near real-time music metadata to provide actionable insights into artist performance, track longevity, and genre dominance.

## Project Overview
This project addresses the fragmentation of music data by unifying two major sources:
* **Kworb.net:** Historical streaming charts and regional performance data.
* **Spotify Web API:** Near real-time track metadata, popularity scores, and audio features.

The architecture follows a modern **ELT (Extract, Load, Transform)** pattern, utilizing a robust tech stack to ensure idempotency, data quality, and scalability.

## Tech Stack
* **Orchestration:** Apache Airflow 2.x
* **Data Warehouse:** Snowflake (AWS)
* **Transformation:** dbt (data build tool)
* **Visualization:** Tableau Public & Preset (Apache Superset)
* **Language:** Python (for API ingestion and scraping)

## Architecture & Data Flow
1. **Ingestion (Airflow):** * `spotify_to_snowflake_raw`: A daily DAG that fetches track data via the Spotify Search API.
    * `kworb_historical_backfill`: A manually triggered DAG that scrapes historical weekly chart data from Kworb.
2. **Raw Layer (Snowflake):** Data is landed in its original form (JSON/HTML-parsed) into a `RAW` schema to ensure traceability and allow for reprocessing.
3. **Transformation Layer (dbt):**
    * **Staging:** Cleans, standardizes, and casts raw data into structured formats.
    * **Analytics/Mart:** Business-level models that join Kworb and Spotify data using fuzzy name matching.
4. **Data Quality (dbt tests):** Automated schema and relationship tests (unique, not_null, etc.) run as a final step in the pipeline.
5. **Consumption:** Cleaned datasets are surfaced in interactive dashboards for trend analysis and KPI tracking.

## Key Features
* **Idempotency:** All Snowflake loads use transactional `BEGIN/COMMIT/ROLLBACK` patterns with `DELETE + INSERT` logic to prevent duplicates on re-runs.
* **Fuzzy Matching:** A custom dbt model matches records between platforms with a confidence threshold (0.75+), bridging the gap between disparate data sources.
* **Scalability:** Airflow Variables and Connections allow for adding new genres or markets without modifying core code.

## Performance Insights
* **Top Artist:** Michael Jackson (42M+ aggregated streams).
* **Top Genre:** Pop (~36M streams), followed by Latin and Rock.
* **Track Longevity:** The system identifies "evergreen" tracks (e.g., *Another Love* by Tom Odell) that have remained on charts for over 3,000 days.

## Project Structure
```text
├── airflow/
│   ├── dags/
│   │   ├── spotify_to_snowflake_raw.py
│   │   └── kworb_historical_backfill.py
│   └── ...
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── analytics/
│   ├── schema.yml
│   └── dbt_project.yml
├── docker-compose.yml
└── README.md
