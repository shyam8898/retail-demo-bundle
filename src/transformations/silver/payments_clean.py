"""Silver (private intermediate): valid, deduplicated payments.

Deduplication: keep the latest occurrence per payment_id (batch date, then
ingestion time, break ties). Data quality: rows failing any payment rule are
dropped here (visible in the pipeline's expectation metrics) and captured for
review in silver.payments_quarantine.

Demo violations: PM17 has a negative amount, PM18 uses an unsupported method
('crypto'). Neither flows into fact_orders or the gold layer.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F


@dp.materialized_view(
    name="payments_clean",
    private=True,
    comment="Valid, deduplicated payments used to enrich fact_orders.",
)
@dp.expect_all_or_drop(
    {
        "payment_amount is non-negative": "payment_amount >= 0",
        "payment_method is card, cash or upi": (
            "payment_method IN ('card', 'cash', 'upi')"
        ),
        "payment_status is valid": (
            "payment_status IN ('completed', 'pending', 'failed', 'refunded')"
        ),
    }
)
def payments_clean():
    payments = spark.read.table("retail_demo.bronze.payments").drop("_rescued_data")

    latest = Window.partitionBy("payment_id").orderBy(
        F.col("_batch_date").desc(), F.col("_ingested_at").desc()
    )
    return (
        payments.withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_batch_date", "_source_file")
    )
