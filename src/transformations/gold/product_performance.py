"""Gold: product performance (materialized view).

Per product and category: units sold, total revenue, distinct orders and
average discount over valid (non-cancelled) order lines.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="retail_demo.gold.product_performance",
    comment=(
        "Per product and category: units sold, revenue, distinct orders and "
        "average discount over non-cancelled, valid order lines."
    ),
)
def product_performance():
    items = (
        spark.read.table("retail_demo.silver.fact_order_items")
        .filter("order_status <> 'cancelled'")
    )
    return items.groupBy("product_id", "product_name", "category").agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("line_revenue").alias("total_revenue"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
    )
