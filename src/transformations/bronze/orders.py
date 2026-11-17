"""Bronze: raw append-only order extracts from the landing volume.

Schema-evolution demo: the second batch (load_20261117.csv) introduces a new
`delivery_notes` column that the first batch does not have. Auto Loader merges
the schemas across files, so bronze.orders gains the column automatically.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_demo.bronze.orders",
    comment=(
        "Raw order extracts: append-only, audit columns "
        "(_source_file, _ingested_at, _batch_date) and automatic schema evolution "
        "(delivery_notes appears from the 2026-11-17 batch onward)."
    ),
)
def orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/retail_demo/raw/landing/orders")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
            F.to_date(
                F.regexp_extract(F.col("_metadata.file_path"), r"load_(\d{8})", 1),
                "yyyyMMdd",
            ).alias("_batch_date"),
        )
    )
