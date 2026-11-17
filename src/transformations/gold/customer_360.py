"""Gold: customer 360 view (materialized view).

One row per current customer (latest SCD2 version) with lifetime order
count, lifetime revenue, items purchased, first / last order date and current
attributes. Customers with only cancelled orders show zero lifetime activity.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="retail_demo.gold.customer_360",
    comment=(
        "Per current customer: lifetime orders, revenue, items, first/last "
        "order date, current segment and home city."
    ),
)
def customer_360():
    items = (
        spark.read.table("retail_demo.silver.fact_order_items")
        .filter("order_status <> 'cancelled'")
    )

    metrics = items.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("lifetime_orders"),
        F.sum("line_revenue").alias("lifetime_revenue"),
        F.sum("quantity").alias("lifetime_items"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )

    customers_current = (
        spark.read.table("retail_demo.silver.dim_customers")
        .filter("__END_AT IS NULL")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            F.col("city").alias("home_city"),
            F.col("state").alias("home_state"),
            "customer_segment",
        )
    )

    return (
        customers_current.join(metrics, "customer_id", "left")
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "home_city",
            "home_state",
            "customer_segment",
            F.coalesce(F.col("lifetime_orders"), F.lit(0)).alias("lifetime_orders"),
            F.coalesce(F.col("lifetime_revenue"), F.lit(0)).alias("lifetime_revenue"),
            F.coalesce(F.col("lifetime_items"), F.lit(0)).alias("lifetime_items"),
            "first_order_date",
            "last_order_date",
        )
    )
