# utils.py
import requests
import os
from pyspark.sql.functions import lit, to_date
from pyspark.sql import SparkSession

def download_to_dbfs(url, dbfs_path):
    os.makedirs(os.path.dirname(f"/dbfs{dbfs_path}"), exist_ok=True)
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(f"/dbfs{dbfs_path}", "wb") as f:
        for chunk in resp.iter_content(1024*1024):
            f.write(chunk)

def read_zip_to_df(spark: SparkSession, path: str, schema=None, sep=";"):
    reader = spark.read.option("header", True).option("sep", sep)
    if schema:
        reader = reader.schema(schema)
    return reader.csv(path)

def add_metadata(df, name, year, month):
    return (df.withColumn("source_file", lit(name))
              .withColumn("reference_dt", to_date(lit(f"{year}-{month}-01"))))