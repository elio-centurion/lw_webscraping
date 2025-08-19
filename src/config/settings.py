from configparser import ConfigParser
from google.cloud import bigquery,storage
import os
import ast
from pathlib import Path

""""
--------------
SETTINGS
--------------
"""

ROOT_PATH=Path(__file__).parent.absolute()
config=ConfigParser(interpolation=None)
config_file_path = ROOT_PATH / "config.ini"
config.read(config_file_path)
big_query=config["bigquery_prod"]
project = big_query["project"]
dataset_cleaned = big_query["dataset_cleaned"]
dataset_raw = big_query["dataset_raw"]
dataset_dwh=big_query["dataset_dwh"]
table = big_query["table"]
table_dwh=big_query["table_dwh"]

def bq_table(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"

def bq_sql_table(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"

TARGET = bq_table(project, dataset_cleaned, table)
TARGET_sql = bq_sql_table(project, dataset_cleaned, table)

LANDING=bq_table(project, dataset_raw, table)
LANDING_sql=bq_sql_table(project, dataset_raw, table)

DWH_TABLE=bq_table(project, dataset_dwh, table_dwh)
DWH_TABLE_sql=bq_sql_table(project, dataset_dwh, table_dwh)

client = bigquery.Client(project=project)
project_id = config["google_storage_prod"]["project"]
bucket_name = config["google_storage_prod"]["bucket"]
storage_client = storage.Client(project=project_id)