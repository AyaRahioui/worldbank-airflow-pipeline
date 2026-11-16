import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from extract import build_csv

PROJECT = "worldbank-gdp-pipeline"
BUCKET = f"{PROJECT}-worldbank-raw"
LOCATION = "europe-west9"


def extract_and_upload_to_gcs(ds, **_):
    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, "indicators.csv")
        n = build_csv(ds, local)
        GCSHook().upload(
            bucket_name=BUCKET,
            object_name=f"worldbank/{ds}/indicators.csv",
            filename=local,
        )
    print(f"{n} lignes uploadées vers gs://{BUCKET}/worldbank/{ds}/indicators.csv")


with DAG(
    "worldbank_gdp_ingestion",
    schedule="@weekly",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw_worldbank.indicators",
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "year", "type": "INT64", "mode": "NULLABLE"},
            {"name": "value", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
        ],
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
        location=LOCATION,
    )

    quality_check = BigQueryCheckOperator(
        task_id="quality_check",
        sql=f"SELECT COUNT(DISTINCT country_code) = 5 FROM `{PROJECT}.raw_worldbank.indicators`",
        use_legacy_sql=False,
        location=LOCATION,
    )

    extract_and_upload >> load_to_bigquery >> quality_check
