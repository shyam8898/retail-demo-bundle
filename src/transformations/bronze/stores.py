"""Bronze: raw append-only store reference extracts from the landing volume."""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="retail_demo.bronze.stores",
    comment=(
        "Raw store reference extracts: append-only, audit columns "
        "(_source_file, _ingested_at, _batch_date) and automatic schema evolution."
    ),
)
def stores():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/Volumes/retail_demo/raw/landing/stores")
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
