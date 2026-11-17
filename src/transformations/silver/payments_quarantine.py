"""Silver (data quality): quarantined payment rows.

Every payment that fails a DQ rule (negative amount, unsupported method,
invalid status) lands here for review instead of silently disappearing.
Demo: PM17 (negative amount) and PM18 (crypto method).
"""
from pyspark import pipelines as dp


@dp.materialized_view(
    name="retail_demo.silver.payments_quarantine",
    comment=(
        "DQ quarantine: payments failing amount / method / status rules. "
        "These rows never flow into fact_orders or the gold layer."
    ),
)
def payments_quarantine():
    payments = spark.read.table("retail_demo.bronze.payments").drop("_rescued_data")
    return payments.filter(
        "payment_amount < 0 "
        "OR payment_method NOT IN ('card', 'cash', 'upi') "
        "OR payment_status NOT IN ('completed', 'pending', 'failed', 'refunded')"
    ).select(
        "payment_id",
        "order_id",
        "payment_method",
        "payment_status",
        "payment_amount",
        "payment_ts",
        "_source_file",
        "_batch_date",
    )
