"""Bronze: raw append-only customer extracts from the landing volume.

Ingestion pattern (identical for every bronze table):
  * Auto Loader streams every CSV that lands in the entity's landing folder.
  * Append-only: new files add rows; existing rows are never rewritten.
  * Audit columns record what file each row came from, when it was ingested,
    and which daily batch the file belongs to (parsed from the file name and
    reused downstream as the CDC / dedup sequence).
  * Schema evolution: a column appearing in a later file is added automatically.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_demo.bronze.customers",
    comment=(
        "Raw customer extracts: append-only, audit columns "
        "(_source_file, _ingested_at, _batch_date) and automatic schema evolution."
    ),
)
def customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/retail_demo/raw/landing/customers")
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
