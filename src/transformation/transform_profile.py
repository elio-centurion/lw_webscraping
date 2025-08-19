import os
import pathlib
from pathlib import Path
import polars as pl
import re
import json
from pathlib import Path
from transformation_util import get_last_part,get_separate_columns_for_variables,get_education_attribute
from polars import DataFrame

def transformations_profile()->pl.DataFrame:
    ROOT_PATH = Path.cwd()
    ROOT_PATH = ROOT_PATH.parent
    profiles_json=os.path.join(ROOT_PATH,'lw_profile.json')
    with open(profiles_json, encoding="utf-8") as f:
        data = json.load(f)
    df = pl.DataFrame(data)
    df=get_last_part(df,'vcard_link',symbol='=')
    df=df.drop('vcard_link')
    df=get_last_part(df,'url','/')
    df=df.with_columns(
        pl.when(pl.col("vcard_link_1").is_null())
        .then(pl.col("url_1"))
        .otherwise(pl.col("vcard_link_1"))
        .alias('id')
    )
    df=df.unique('id')
    df=get_education_attribute(df)
    df=get_separate_columns_for_variables(df,column_name='telephones')
    return df

def transformation_news()->pl.DataFrame:
    ROOT_PATH = Path.cwd()
    profiles_json=os.path.join(ROOT_PATH,'lw_news.json')
    with open(profiles_json, encoding="utf-8") as f:
        data = json.load(f)
    df = pl.DataFrame(data)
    df=df.unique('url')
    df=df.with_columns(
        pl.col("date_article").str.strptime(pl.Date,format="%B %d, %Y",strict=False)
        .dt.strftime("%Y-%m-%d")
        .alias("date_article")
    )
    df.explode('persons_url')
    return df

df_profile=transformations_profile()
with pl.Config(tbl_cols=-1):
    print(df_profile)
df_news=transformation_news()
df_join = df_profile.join(df_news, how="left", left_on="name", right_on="persons_urL")
df_join=df_join.select(['name','jobTitle','email','locations','education','jd_entry','has_jd','url','id','telehones_0',
                        'telehones_1','telehones_1','practices','industries',''])
df_join=df_join.group_by('name','jobTitle','email','locations','education','jd_entry','has_jd','url','id','telehones_0',
                         'telehones_1','telehones_1','practices','industries') \
         .agg(pl.col('headline'))
df_join.write_excel(workbook='profiles.xlsx',autofit=True)