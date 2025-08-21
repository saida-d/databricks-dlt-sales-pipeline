import dlt

# create empty stereaming table
dlt.create_streaming_table(
    name="dim_customers"
)

# create cdc_flow to access silver customers view
dlt.create_auto_cdc_flow(
    target="dim_customers",
    source="customers_silver_trf_view",  # from silver read view (not silver table since)
    keys=["customer_id"],
    sequence_by="last_updated",
    ignore_null_updates=False,
    apply_as_deletes=None,
    apply_as_truncates=None,
    column_list=None,
    except_column_list=None,
    stored_as_scd_type=2, # type2 for history with changes
    track_history_column_list=None,
    track_history_except_column_list=None
)