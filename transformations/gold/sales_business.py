import dlt
from pyspark.sql.functions import *

# materialized view
@dlt.table(
  name="sales_business_rp",
  comment="This view contains all the data from the silver views"
)
def business_sales():
    df_dim_sales=spark.read.table("sales_fact")
    df_dim_customers=spark.read.table("dim_customers")
    df_dim_products=spark.read.table("dim_products")
    df_sales_join=df_dim_sales.join(df_dim_customers, df_dim_sales.customer_id == df_dim_customers.customer_id,"inner").join(df_dim_products, df_dim_sales.product_id == df_dim_products.product_id,"inner")
    df_sel=df_sales_join.select("region","category","amount")
    df_agg=df_sel.groupBy("region","category").agg(sum("amount").alias("total_sales"))

    return df_agg