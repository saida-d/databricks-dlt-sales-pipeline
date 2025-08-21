import dlt

customer_rules={
    'rule_1':'region IS NOT NULL'
}

dlt.create_streaming_table(name="os_customers_append", expect_all_or_drop=customer_rules)

@dlt.append_flow(target="os_customers_append")
def os_customers():
    return spark.readStream.table("dltcatalog.default.customers")