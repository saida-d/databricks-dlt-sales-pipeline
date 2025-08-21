import dlt
from pyspark.sql.functions import *

# create a streaming table (empty)
dlt.create_streaming_table(name="customers_silver_tbl")

# create a customers view for gold layer access (filtered data)
@dlt.view(name="customers_silver_trf_view")
# spark.readStream is recommended to @dlt.streaming_view
# @dlt.view still supports spark.readStream for backward compatibility
def customers_silver():
  df= spark.readStream.table("os_customers_append") # pointed from bronze stream table
  df= df.filter(col("customer_name").isNotNull())
  return df

# create CDC (auto CDC flow)
# create silver table from bronze
'''
Key notes:
1. Works only in DLT pipelines, not in ad-hoc Spark jobs.
2. Simplifies CDC handling, no need for MERGE boilerplate.
3. Typically applied in Silver layer to normalize CDC → then Gold consumes it.
'''
dlt.create_auto_cdc_flow(
    target="customers_silver_tbl",
    source="customers_silver_trf_view",  # creating view from bronze for filtering and send to gold layer
    keys=["customer_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=1,
    track_history_column_list=None,
    track_history_except_column_list=None
)