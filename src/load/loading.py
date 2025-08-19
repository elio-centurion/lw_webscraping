# pip install google-cloud-bigquery pandas pyarrow
import os
import time
import datetime, pytz, random
import io
from typing import Optional

import polars as pl
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta
from src.config.settings import TARGET, client, ROOT_PATH, \
    project, dataset_cleaned, table, LANDING, dataset_raw, LANDING_sql, TARGET_sql, DWH_TABLE_sql
from .bigquery_functions import delete_temp_table
from .upserts import from_polars_to_temp, upsert_from_temp_to_target, insert_from_temp_to_target, \
    upsert_from_target_to_dwh


def loading_to_target(df:pl.DataFrame)->bigquery.job.QueryJob:
    temp_id, temp_name = from_polars_to_temp(df, dataset_cleaned, table, project, client,cleaned_procces=True)
    job=upsert_from_temp_to_target(target=TARGET_sql,df=df,tmp_table=temp_name)
    delete_temp_table(table_id=temp_name)
    return job

def loading_raw(df:pl.DataFrame):
    temp_id, temp_name = from_polars_to_temp(df, dataset_raw, table_name=table, project=project, client=client,cleaned_procces=False)
    insert_from_temp_to_target(target=LANDING_sql,tmp_table=temp_name)
    delete_temp_table(table_id=temp_name)

def loading(df: pl.DataFrame):
    """Run a merge if something changed at the target table"""
    merge_job=loading_to_target(df)
    dwh_job=None

    stats=getattr(merge_job,"dml_stats", None)
    inserted=getattr(stats,"inserted_row_count", 0)
    updated=getattr(stats,"updated_row_count", 0)
    if inserted or updated:
        dwh_job=upsert_from_target_to_dwh(dwh_table=DWH_TABLE_sql,source_table=TARGET_sql,site_page='lw')
    return merge_job, dwh_job


