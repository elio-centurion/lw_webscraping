from datetime import datetime, timezone, date
from pathlib import Path
import os
import json
import polars as pl

def transform_array_into_list(df:pl.DataFrame,column_name):
    df = df.with_columns(
    pl.when(pl.col((f"{column_name}")).is_null())
      .then(pl.lit([]).cast(pl.List(pl.Utf8)))
      .otherwise(pl.col((f"{column_name}")).cast(pl.List(pl.Utf8)))
      .alias((f"{column_name}"))
    )
    return df

def make_hash_cols(df:pl.DataFrame):
    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("headline"),
                pl.col("news_type").fill_null(""),
                pl.col("datePublished").cast(pl.Utf8).fill_null(""),
                pl.col("lawyer_link").list.join(",").fill_null(""),
                pl.col("lawyer_names").list.join(",").fill_null(""),
                pl.col("region").fill_null(""),
                pl.col("content_date_watermark").cast(pl.Utf8).fill_null(""),
            ],
            separator="||"
        ).hash(seed=0).alias("hash_cols").cast(pl.Utf8)
    )
    return df

def transformation_news(all_news:dict)->pl.DataFrame:
    df = pl.DataFrame(all_news)
    df = df.unique('permanentid')
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime).alias("syncstartdatetime"))
    df=df.with_columns(
        pl.col("datePublished").str.strptime(pl.Date,format="%B %d, %Y",strict=False)
        .dt.strftime("%Y-%m-%d").cast(date)
        .alias("datePublished")
    )
    df=transform_array_into_list(df,"lawyer_link")
    df = transform_array_into_list(df, "lawyer_names")
    df=df.with_columns(pl.from_epoch("contentdate", time_unit="ms").alias("content_date_watermark"))
    df=df.with_columns(pl.col("permanentid").alias('id'))
    df=df.with_columns(pl.lit('lw').alias('site_page'))
    df=make_hash_cols(df)
    df=df.with_columns(pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime).alias("syncstartdatetime"))
    df=df.drop(pl.col(['permanentid','contentdate']))
    return df

def transformation_news_raw(all_news:dict)->pl.DataFrame:
    df = pl.DataFrame(all_news)
    df=df.unique('permanentid')
    df = df.with_columns(pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime).alias("syncstartdatetime"))
    return df

def transformations(all_news:dict):
    df=transformation_news(all_news)
    df_raw=transformation_news_raw(all_news)
    return df,df_raw