"""Silver: SCD Type 2 customer dimension.

Auto CDC diffs the append-only bronze stream by customer_id and keeps full
history: a change to any tracked attribute opens a new version (previous
versions get an __END_AT, the newest row has __END_AT IS NULL).

Demo scenario: customer C003's segment changes retail -> premium between the
2026-11-16 and 2026-11-17 loads, so dim_customers keeps two versions of C003,
only the newer one current. C001 is re-dropped unchanged, which does NOT open
a new version (nothing tracked changed).
"""
from pyspark import pipelines as dp


dp.create_streaming_table(
    name="retail_demo.silver.dim_customers",
    comment=(
        "SCD Type 2 customer dimension: full history per customer_id via "
        "__START_AT / __END_AT (current row has __END_AT IS NULL)."
    ),
)

dp.create_auto_cdc_flow(
    target="retail_demo.silver.dim_customers",
    source="retail_demo.bronze.customers",
    keys=["customer_id"],
    sequence_by="_batch_date",
    stored_as_scd_type=2,
    track_history_column_list=[
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "customer_segment",
    ],
    except_column_list=["_rescued_data"],
)
