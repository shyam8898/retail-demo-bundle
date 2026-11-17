"""Silver (data quality): quarantined order-line rows.

Every order line that fails a DQ rule (non-positive quantity, negative unit
price, missing keys) lands here for review instead of silently disappearing.
Demo: OI27 (quantity 0) and OI28 (quantity -2).
"""
from pyspark import pipelines as dp


@dp.materialized_view(
    name="retail_demo.silver.order_items_quarantine",
    comment=(
        "DQ quarantine: order lines failing quantity / price / key rules. "
        "These rows never flow into the gold layer."
    ),
)
def order_items_quarantine():
    items = spark.read.table("retail_demo.bronze.order_items").drop("_rescued_data")
    return items.filter(
        "quantity <= 0 "
        "OR unit_price < 0 "
        "OR order_item_id IS NULL "
        "OR order_id IS NULL"
    ).select(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_pct",
        "_source_file",
        "_batch_date",
    )
