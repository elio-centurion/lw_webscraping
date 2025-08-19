import calendar
from datetime import datetime, timedelta, time, date, timezone
from google.cloud import bigquery
from src.config.settings import TARGET, client


def get_tomorrows_date():
    today=datetime.now(timezone.utc)
    tomorrow=today+timedelta(days=1)
    end_of_day=datetime.combine(tomorrow.date(),time.max,tzinfo=timezone.utc) \
        .isoformat(timespec='milliseconds').replace("+00:00","Z")
    return end_of_day

def flag_of_first_day_of_month()->bool:
    now = datetime.now(timezone.utc)
    return now.day == 1 and now.hour == 4

def get_first_of_month():
    now=datetime.now(timezone.utc)
    month=now.month-1
    start_of_month = now.replace(month=month,day=1,hour=0,minute=0,second=0,microsecond=0) \
        .isoformat(timespec='milliseconds')\
        .replace('+00:00','Z')
    return start_of_month

def get_last_startdate(client):
    now=datetime.now(timezone.utc).date()
    last_day=now-timedelta(days=7)
    query = f"""
    SELECT MAX(datePublished) AS max_date
    FROM {TARGET}
    WHERE datePublished >= @last_day
    """
    job_cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("last_day", "DATE", last_day)]
    )
    rows = client.query(query, job_config=job_cfg).result()
    row = next(rows, None)
    last_startdate=row.max_date if row else None
    if last_startdate is not None:
        #in unix because at the webpage it is in unix milisecond forma
        last_startdate = datetime.combine(last_startdate, datetime.min.time(), tzinfo=timezone.utc)
        last_startdate=int(last_startdate.timestamp() * 1000)
        return last_startdate