"""Silver: deduplicated, enriched order-line facts with data quality rules.

Deduplication: keeps the latest occurrence per order_item_id. Data quality:
lines with a non-positive quantity or negative unit price are dropped (visible
in expectation metrics and captured in silver.order_items_quarantine).

Demo violations: OI27 has quantity 0, OI28 has quantity -2; neither reaches
the gold layer, so sales KPIs are not corrupted.

line_revenue = quantity * unit_price * (1 - discount_pct / 100).
"""
from pyspark import pipelines as dp
from pyspark.sql import Window, functions as F


@dp.materialized_view(
    name="retail_demo.silver.fact_order_items",
    comment=(
        "One row per order line: deduplicated on order_item_id (latest wins), "
        "enriched with product and order attributes, with DQ rules dropping "
        "invalid quantities and prices."
    ),
)
@dp.expect_all_or_drop(
    {
        "order_item_id is present": "order_item_id IS NOT NULL",
        "order_id is present": "order_id IS NOT NULL",
        "quantity is positive": "quantity > 0",
        "unit_price is non-negative": "unit_price >= 0",
    }
)
def fact_order_items():
    items = spark.read.table("retail_demo.bronze.order_items").drop("_rescued_data")

    # Dedup: latest occurrence per order_item_id wins.
    latest = Window.partitionBy("order_item_id").orderBy(
        F.col("_batch_date").desc(), F.col("_ingested_at").desc()
    )
    items_dedup = (
        items.withColumn("_rn", F.row_number().over(latest))
        .filter("_rn = 1")
        .drop("_rn", "_ingested_at", "_batch_date", "_source_file")
    )

    products = spark.read.table("retail_demo.silver.dim_products").select(
        "product_id", "product_name", "category", "brand"
    )
    orders = spark.read.table("retail_demo.silver.fact_orders").select(
        "order_id", "customer_id", "order_date", "order_channel", "order_status"
    )

    return (
        items_dedup.join(products, "product_id", "left")
        .join(orders, "order_id", "left")
        .withColumn(
            "line_revenue",
            F.expr("quantity * unit_price * (1 - discount_pct / 100)"),
        )
        .select(
            "order_item_id",
            "order_id",
            "order_status",
            "customer_id",
            "order_date",
            "order_channel",
            "product_id",
            "product_name",
            "category",
            "brand",
            "quantity",
            "unit_price",
            "discount_pct",
            "line_revenue",
        )
    )
