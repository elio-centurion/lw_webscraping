import polars as pl

def get_last_part(df:pl.DataFrame,col_name:str,symbol:str)->pl.DataFrame:
    df=df.with_columns(
        pl.col(col_name).str.split(by=symbol).list.last().alias(f'{col_name}_1'))
    return df

def get_separate_columns_for_variables(df:pl.DataFrame,column_name:str)-> pl.DataFrame:
    over_limit=df.filter(pl.col(f"{column_name}").list.len() > 3)
    if over_limit.height>0:
        logger.warning(f"The column {column_name} is over limit: {over_limit.height}")
    for i in range(3):
        df = df.with_columns(
            pl.col(f"{column_name}").list.get(i, null_on_oob=True).alias(f"{column_name}_{i}"))
    return df


def get_education_attribute(df:pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        pl.col("education").map_elements(
            lambda ed: next((s for s in ed if "JD" in s), None),
            return_dtype=pl.Utf8
        ).alias("jd_entry")
    )
    df = df.with_columns(
        pl.when(pl.col("jd_entry").is_not_null())
        .then(pl.lit("yes"))
        .otherwise(pl.lit("no"))
        .alias("has_jd")
    )
    df = df.with_columns(
        pl.col("jd_entry")
        .str.extract(r"(\b\d{4}\b)", 1)
        .cast(pl.Int32)
        .alias("jd_year")
    )
    return df
