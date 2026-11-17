"""Silver: deduplicated, enriched order facts (one row per order).

Deduplication: keeps the latest occurrence per order_id. Demo: O1001 is
re-dropped in the 2026-11-17 batch and appears exactly once here, with the
newer row (including delivery_notes) winning.

Enrichment: store attributes, the customer's current (SCD2) attributes, and
the latest valid payment per order from payments_clean. Cancelled orders are
kept in silver for completeness; the gold layer excludes them from sales KPIs.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F


@dp.materialized_view(
    name="retail_demo.silver.fact_orders",
    comment=(
        "One row per order: deduplicated on order_id (latest batch wins), "
        "enriched with store, current-customer and latest valid payment attributes."
    ),
)
@dp.expect_all_or_drop(
    {
        "order_id is present": "order_id IS NOT NULL",
        "customer_id is present": "customer_id IS NOT NULL",
        "store_id is present": "store_id IS NOT NULL",
        "order_date is present": "order_date IS NOT NULL",
    }
)
def fact_orders():
    orders = spark.read.table("retail_demo.bronze.orders").drop("_rescued_data")

    # Dedup: latest occurrence per order_id wins.
    latest = Window.partitionBy("order_id").orderBy(
        F.col("_batch_date").desc(), F.col("_ingested_at").desc()
    )
    orders_dedup = (
        orders.withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_batch_date", "_source_file")
    )

    stores = (
        spark.read.table("retail_demo.bronze.stores")
        .drop("_rescued_data")
        .select(
            "store_id",
            "store_name",
            F.col("city").alias("store_city"),
            "region",
            "store_type",
        )
    )

    customers_current = (
        spark.read.table("retail_demo.silver.dim_customers")
        .filter("__END_AT IS NULL")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            F.col("city").alias("customer_city"),
            F.col("state").alias("customer_state"),
            "customer_segment",
        )
    )

    payments = spark.read.table("payments_clean")
    latest_payment = Window.partitionBy("order_id").orderBy(
        F.col("payment_ts").desc()
    )
    payments_latest = (
        payments.withColumn("_rn", F.row_number().over(latest_payment))
        .filter("_rn = 1")
        .drop("_rn")
        .select("order_id", "payment_method", "payment_status", "payment_amount")
    )

    return (
        orders_dedup.join(stores, "store_id")
        .join(customers_current, "customer_id", "left")
        .join(payments_latest, "order_id", "left")
        .select(
            "order_id",
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "customer_city",
            "customer_state",
            "customer_segment",
            "store_id",
            "store_name",
            "store_city",
            "region",
            "store_type",
            "order_date",
            "order_status",
            "order_channel",
            "delivery_notes",
            "payment_method",
            "payment_status",
            "payment_amount",
        )
    )
