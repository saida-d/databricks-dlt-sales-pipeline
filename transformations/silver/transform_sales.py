import dlt
from pyspark.sql.functions import col

# sales view from bronze with transformation as new column (total_sales= unit_price*quantity)
# creating view from bronze for filtering and send to gold layer (or) you can use streaming view 
# and send that streaming view to gold layer
@dlt.view(name="sales_silver_trf_view")
def os_sales_append():
    df= spark.readStream.table("os_sales_append") # read from bronze
    df= df.withColumn("total_sales", col("amount")*col("quantity"))
    return df

# create silver destination live table
dlt.create_streaming_table(name="sales_silver_trf")

'''
e.g. other way to create table
@dlt.create_auto_cdc_flow
def customers_cdc():
    return (
        spark.readStream.format("delta")
        .table("bronze_customers")
    )
'''

# create silver table from bronze
'''
Key notes:
1. Works only in DLT pipelines, not in ad-hoc Spark jobs.
2. Simplifies CDC handling, no need for MERGE boilerplate.
3. Typically applied in Silver layer to normalize CDC → then Gold consumes it.
'''
dlt.create_auto_cdc_flow(
    target="sales_silver_trf",
    source="sales_silver_trf_view",  # creating view from bronze for filtering and send to gold layer
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None
)