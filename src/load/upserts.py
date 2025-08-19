import os
import time
import datetime, pytz, random
import io
from typing import Optional

import polars as pl
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from datetime import datetime, timezone, timedelta

from google.cloud.bigquery import ParquetOptions

from src.config.settings import TARGET, client, ROOT_PATH, \
    project, dataset_cleaned, table, LANDING, dataset_raw, LANDING_sql, TARGET_sql, DWH_TABLE_sql
from .bigquery_functions import get_temp_table

KEY_COLS = ("id",)
date_col="datePublished"
hash_col="hash_cols"
site_page='lw'

def from_polars_to_temp(df: pl.DataFrame | pl.LazyFrame,dataset: str, table_name: str, project: str,
                        client: bigquery.Client,cleaned_procces:bool=False):
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if df.height == 0:
        print("DF is empty — nothing to upsert.")
        return None
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    parquet_options = bigquery.ParquetOptions()
    if cleaned_procces:
        parquet_options.enable_list_inference = True
    tmp_table_def,tmp_table_name = get_temp_table(dataset=dataset, table_name=table_name, project=project)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        parquet_options=parquet_options
    )
    job = client.load_table_from_file(buf, tmp_table_name, job_config=job_config)
    job.result()
    return tmp_table_def,tmp_table_name


def insert_from_temp_to_target(target, tmp_table):
    tgt = client.get_table(target)
    cols = [f"`{f.name}`" for f in tgt.schema]
    col_list = ", ".join(cols)
    sql = f"""
    INSERT INTO {target} ({col_list})
    SELECT {col_list}
    FROM {tmp_table}
    """
    job=client.query(sql)
    job.result()
    return job

def upsert_from_temp_to_target(target:str,df:pl.DataFrame,tmp_table:str):
    target_obj=client.get_table(target)
    target_cols = [f.name for f in target_obj.schema]
    source_cols=df.columns
    non_key_cols=[col for col in source_cols if col not in KEY_COLS]
    set_clause = ",\n        ".join([f"{col} = S.{col}" for col in non_key_cols])
    insert_cols=", ".join(source_cols)
    insert_vals=", ".join([f"S.{c}" for c in source_cols])
    today=datetime.now(timezone.utc).date()
    date_difference=today-timedelta(days=7)

    on_pred = " AND ".join([f"T.{k} = S.{k}" for k in KEY_COLS])
    sql = f"""
        MERGE {target} AS T
        USING {tmp_table} AS S
        ON {on_pred} AND T.{date_col} >= @date_difference
        WHEN MATCHED AND T.{hash_col}<>S.{hash_col} THEN
          UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals});
        """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date_difference", "DATE", date_difference)]
        ),
    )
    job.result()
    return job

def upsert_from_target_to_dwh(dwh_table:str,source_table:str,site_page:str):
    """Merge source table (in our case the cleaned) into the dwh table if there was any DML operations done"""
    src_obj=client.get_table(source_table)
    src_col = [f.name for f in src_obj.schema]
    dwh_obj=client.get_table(dwh_table)
    dwh_cols   = {f.name for f in dwh_obj.schema}
    common_cols = [c for c in src_col if c in dwh_cols]
    non_key_cols=[c for c in common_cols if c not in KEY_COLS]
    set_clause = ",\n        ".join([f"{col} = S.{col}" for col in non_key_cols])

    insert_cols=", ".join(common_cols)
    insert_vals=", ".join([f"S.{c}" for c in common_cols])
    on_pred = " AND ".join([f"T.{k} = S.{k}" for k in KEY_COLS])
    sql = f"""
        MERGE {dwh_table} AS T
        USING {source_table} AS S
        ON {on_pred} AND T.site_page=@site_page
        WHEN MATCHED THEN
          UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals});
        """

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("site_page", "STRING", site_page)]
        ),
    )
    job.result()
    return job
