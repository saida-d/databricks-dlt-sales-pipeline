import dlt

prod_rules={
    'rule_1':'product_id IS NOT NULL'
}

dlt.create_streaming_table(name="os_products_append", expect_all_or_drop=prod_rules)

@dlt.append_flow(target="os_products_append")
def os_products():
    return spark.readStream.table("dltcatalog.default.products")