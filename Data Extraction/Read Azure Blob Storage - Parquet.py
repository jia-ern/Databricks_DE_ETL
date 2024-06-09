# Databricks notebook source
# Date Range
from datetime import date, timedelta

today = date.today()
last_month = today.replace(day=1) - timedelta(days=1)
year = last_month.year
month = last_month.month  
start_date = date(year, month, 1)

# COMMAND ----------

# Source files
blob_account_name = "blob_account_name"
blob_container_name = "blob_container_name"
blob_sas_token = dbutils.secrets.get("sas_token_name", "sastoken")
blob_relative_path = f"compressed/AWS Send Parquet/{year}/{month}/*.snappy.parquet"

# COMMAND ----------

wasbs_path = f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}"
spark.conf.set(f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net",blob_sas_token)

# COMMAND ----------

# Table & View
raw_send = "raw_send"
send_view = "send_view"
send_data = "send_data"

# COMMAND ----------

# Tax rate
taxRate = 1

# COMMAND ----------

# Get Raw Data from Source files
df = spark.sql(f"""
  SELECT * FROM parquet.`{wasbs_path}`
""")

# COMMAND ----------

# Handle future sc_type
from pyspark.sql.functions import lit

if 'resource_tags_user_sc_type' not in df.columns:
    df = df.withColumn("resource_tags_user_sc_type", lit(None))

df = df.select("bill_billing_period_start_date", "line_item_usage_account_id", "line_item_unblended_cost", "pricing_term", "line_item_resource_id", "product_from_location","product_product_name", "resource_tags_user_sc_system", "resource_tags_user_sc_component", "resource_tags_user_sc_env", "resource_tags_user_sc_region", "resource_tags_aws_created_by", "resource_tags_user_sc_provider", "resource_tags_user_sc_costowner", "resource_tags_user_sc_type")

df.createOrReplaceTempView(send_view)

# COMMAND ----------

spark.sql(f"""
DELETE FROM {raw_send} WHERE DATE_TRUNC('month', bill_billing_period_start_date)::DATE = '{start_date}'
""")

# COMMAND ----------

df.write.format("delta").mode("append").option("delta.columnMapping.mode", "name").saveAsTable(raw_send)

# COMMAND ----------

spark.sql(f"DELETE FROM {send_data} where Year = {year} AND Month = {month}")

# COMMAND ----------

# Append Data
spark.sql(f"""
    INSERT INTO {send_data}
    SELECT
    --System fields
    bill_billing_period_start_date::Date, line_item_usage_account_id, line_item_unblended_cost::DOUBLE / {taxRate}, pricing_term, line_item_resource_id, product_from_location,product_product_name,
    --Tags fields
    resource_tags_user_sc_system, resource_tags_user_sc_component, resource_tags_user_sc_env, resource_tags_user_sc_region, 
    resource_tags_aws_created_by, resource_tags_user_sc_provider, resource_tags_user_sc_costowner, resource_tags_user_sc_type,
    'Send', 'Amazon', YEAR(bill_billing_period_start_date), MONTH(bill_billing_period_start_date) 
    FROM {send_view}
    """)

# COMMAND ----------

dbutils.notebook.exit("OK")
