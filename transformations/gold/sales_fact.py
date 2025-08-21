import dlt

# create empty stereaming table
dlt.create_streaming_table(
    name="sales_fact"
)

# create cdc_flow to access silver sales view
dlt.create_auto_cdc_flow(
    target="sales_fact",
    source="sales_silver_trf_view",  # from silver read view (not silver table since)
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