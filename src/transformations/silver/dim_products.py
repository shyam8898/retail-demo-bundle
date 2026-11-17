"""Silver: SCD Type 1 product dimension.

SCD Type 1 keeps only the latest value per product_id (no history): every
re-drop of a product row simply overwrites the previous state, which is also
how keyed deduplication is achieved.
"""
from pyspark import pipelines as dp


dp.create_streaming_table(
    name="retail_demo.silver.dim_products",
    comment=(
        "SCD Type 1 product dimension: latest values per product_id, no history."
    ),
)

dp.create_auto_cdc_flow(
    target="retail_demo.silver.dim_products",
    source="retail_demo.bronze.products",
    keys=["product_id"],
    sequence_by="_batch_date",
    stored_as_scd_type=1,
    except_column_list=["_rescued_data"],
)
