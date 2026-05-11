from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt"


conn = BaseHook.get_connection("snowflake_music_conn")

with DAG(
    dag_id="music_analytics_dbt_pipeline",
    start_date=datetime(2026, 5, 1),
    description="Run dbt transform and analytics models for music analytics project",
    schedule="15 4 * * *",
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": "ELT",
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake",
        }
    },
    tags=["dbt", "music", "elt"],
) as dag:

    dbt_run_transform = BashOperator(
        task_id="dbt_run_transform",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select transform "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command=(
            f"/home/airflow/.local/bin/dbt run "
            f"--select analytics "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"/home/airflow/.local/bin/dbt test "
            f"--profiles-dir {DBT_PROJECT_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    dbt_run_transform >> dbt_run_analytics >> dbt_test