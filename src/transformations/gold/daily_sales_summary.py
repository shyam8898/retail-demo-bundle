"""Gold: daily sales summary (materialized view).

Sales KPIs per order date and channel over valid (non-cancelled) order lines:
distinct order count, total revenue, items sold and average order value.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="retail_demo.gold.daily_sales_summary",
    comment=(
        "Sales KPIs per order date and channel over non-cancelled, valid "
        "order lines: orders, revenue, items sold, average order value."
    ),
)
def daily_sales_summary():
    items = (
        spark.read.table("retail_demo.silver.fact_order_items")
        .filter("order_status <> 'cancelled'")
    )
    return (
        items.groupBy("order_date", "order_channel")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("line_revenue").alias("total_revenue"),
            F.sum("quantity").alias("items_sold"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_revenue") / F.col("order_count"), 2),
        )
    )
