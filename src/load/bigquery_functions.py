import json

from google.cloud import bigquery
import datetime, pytz, random
from src.config.settings import client, storage_client, bucket_name


def get_temp_table(dataset: str, table_name: str = None, project=None):
    prefix = "temp"
    suffix = random.randint(10000, 99999)
    if not table_name:
        table_name = "noname"

    temp_table_name = f"{dataset}.{prefix}_{table_name}_{suffix}"
    if project:
        temp_table_name = f"{project}.{temp_table_name}"
    tmp_table_def = bigquery.Table(temp_table_name)
    tmp_table_def.expires = datetime.datetime.now(pytz.utc) + datetime.timedelta(
        hours=1
    )
    table = client.create_table(tmp_table_def)
    return table,temp_table_name

def delete_temp_table(table_id: str):
    try:
        client.delete_table(table_id, not_found_ok=True)
    except Exception as e:
        print(f"[cleanup] couldn't delete {table_id}: {e}")

def _ensure_prefix(bucket, prefix: str):
    it = storage_client.list_blobs(bucket.name, prefix=prefix)
    if next(it, None) is None:
        print(f"Creating placeholder: {prefix!r}")
        bucket.blob(prefix if prefix.endswith('/') else prefix + '/').upload_from_string(b'')

def get_storage_locations(year: int, month: int, day: int,obj, dest_name: str):
    bucket = storage_client.bucket(bucket_name)
    year_prefix = f"yyyy={year:04d}/"
    _ensure_prefix(bucket, year_prefix)

    month_prefix = f"{year_prefix}mm={month:02d}/"
    _ensure_prefix(bucket, month_prefix)

    day_prefix = f"{month_prefix}dd={day:02d}/"
    _ensure_prefix(bucket, day_prefix)

    object_name = day_prefix + dest_name
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(obj, ensure_ascii=False), content_type="application/json")
    return object_name

