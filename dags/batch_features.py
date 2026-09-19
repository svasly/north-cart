"""DAG расчёта batch-признаков NorthCart.

Весь расчёт живёт в модуле dags/calculate_batch_features.py. DAG только:
  * берёт run_date из Airflow Variable `batch_features_run_date`;
  * читает сырые таблицы из PostgreSQL (Connection `ecommerce_db`);
  * вызывает единую функцию расчёта (одинаковую для истории и инференса);
  * кладёт срез в S3 (Connection `aws_default`) по ключу run_date=<дата>/... ,
    поэтому прогоны на разных датах не перезаписывают друг друга;
  * проверяет, что файл в S3 существует и не пустой.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from botocore.exceptions import ClientError

# Модуль лежит рядом с DAG: папка dags/ есть в sys.path воркера Airflow
from calculate_batch_features import build_batch_features, load_source_tables

DAG_ID = "batch_features"
POSTGRES_CONN_ID = "ecommerce_db"   # Connection для сырых таблиц (по заданию)
S3_CONN_ID = "aws_default"          # Connection для S3 (все параметры в extra)
DEFAULT_SOURCE_SCHEMA = "public"
DEFAULT_OUTPUT_DIR = "/tmp/batch_features"


def get_postgres_uri() -> str:
    """Возвращает URI подключения к PostgreSQL из Airflow Connection."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    return hook.get_uri()


def get_s3_client_and_bucket() -> tuple:
    """Создаёт S3-клиент и возвращает bucket из Airflow Connection `extra`."""
    connection = BaseHook.get_connection(S3_CONN_ID)
    extras = connection.extra_dejson

    bucket = extras.get("bucket")
    if not bucket:
        raise ValueError(f"Missing required `bucket` in extras for connection `{S3_CONN_ID}`")
    client = boto3.client(
        "s3",
        aws_access_key_id=extras.get("aws_access_key_id"),
        aws_secret_access_key=extras.get("aws_secret_access_key"),
        endpoint_url=extras.get("endpoint_url"),
    )
    return client, bucket


with DAG(
    dag_id=DAG_ID,
    schedule=None,          # только ручной запуск
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch-features"],
) as dag:

    @task(task_id="build_and_upload_features")
    def build_and_upload_features() -> dict:
        """Считывает run_date, считает batch-признаки и загружает результат в S3."""
        run_date = Variable.get("batch_features_run_date")

        # 1. Сырые таблицы из PostgreSQL (секреты приходят из Connection)
        tables = load_source_tables(get_postgres_uri())

        # 2. Единая логика расчёта для ЛЮБОЙ даты среза (история или инференс).
        #    Встроенные проверки validate_features отработают внутри.
        features = build_batch_features(
            tables["customers"],
            tables["sessions"],
            tables["events"],
            tables["orders"],
            run_date,
        )

        # 3. Локальный файл перед загрузкой в S3
        local_path = Path(DEFAULT_OUTPUT_DIR) / f"run_date={run_date}" / "batch_features.csv"
        local_path.parent.mkdir(parents=True, exist_ok=True)
        features.to_csv(local_path, index=False)

        # 4. Загрузка в S3: ключ содержит run_date -> каждый прогон отдельным результатом
        s3_client, s3_bucket = get_s3_client_and_bucket()
        s3_key = f"run_date={run_date}/batch_features.csv"
        s3_client.upload_file(str(local_path), s3_bucket, s3_key)

        return {
            "run_date": run_date,
            "rows": len(features),
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
        }

    @task(task_id="validate_saved_result")
    def validate_saved_result(result_info: dict) -> None:
        """Проверяет, что файл с batch-признаками существует в S3 и не пустой."""
        if int(result_info["rows"]) <= 0:
            raise ValueError("Saved feature file is empty")

        s3_client, _ = get_s3_client_and_bucket()
        s3_bucket = str(result_info["s3_bucket"])
        s3_key = str(result_info["s3_key"])

        try:
            metadata = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        except ClientError as error:
            error_code = error.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(f"S3 object was not found: s3://{s3_bucket}/{s3_key}") from error
            raise

        if int(metadata.get("ContentLength", 0)) <= 0:
            raise ValueError(f"S3 object is empty: s3://{s3_bucket}/{s3_key}")

    validate_saved_result(build_and_upload_features())