import dlt

# rules
sales_rules={
    'rule_1':'product_id IS NOT NULL',
    'rule_2':'customer_id IS NOT NULL'
}

# create empty streaming table
dlt.create_streaming_table(name="os_sales_append", expect_all_or_drop=sales_rules)

@dlt.append_flow(target="os_sales_append")
def sales_east():
    # catalog.schema ref to settings tab
    return spark.readStream.table("dltcatalog.default.sales_east")

@dlt.append_flow(target="os_sales_append")
def sales_west():
    # catalog.schema ref to settings tab
    return spark.readStream.table("dltcatalog.default.sales_west")