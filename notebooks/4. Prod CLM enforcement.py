# Databricks notebook source
# MAGIC %md
# MAGIC # Publishing cleaned and transformed Bronze tables into Production Catalog 
# MAGIC
# MAGIC >For demo purposes only, there are no real data transformations applied below.
# MAGIC
# MAGIC >Please note that Shared Compute should be used to run this notebook due to RLS/CLM limitation

# COMMAND ----------

dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");

dbutils.widgets.text("prod_catalog", "dss_demo_prod", "Target Prod Catalog:");
dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");

dbutils.widgets.text("privileged_group_name", "default", "prod-privileged-users");

# COMMAND ----------

diz_catalog = dbutils.widgets.get("diz_catalog")
diz_schema = dbutils.widgets.get("diz_schema")
prod_catalog = dbutils.widgets.get("prod_catalog")
target_schema = dbutils.widgets.get("target_schema")
privileged_group_name = dbutils.widgets.get("privileged_group_name")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Creation of the Column Mask Function for non authorized group of users to hide the real sensitive values from them

# COMMAND ----------

mask_policy_name = f"{prod_catalog}.{target_schema}.pii_mask"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG $prod_catalog;
# MAGIC USE SCHEMA $target_schema;
# MAGIC CREATE OR REPLACE FUNCTION pii_mask(column STRING) RETURN IF(
# MAGIC     IS_ACCOUNT_GROUP_MEMBER('$privileged_group_name'),
# MAGIC     column,
# MAGIC     hash(column)
# MAGIC   );

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Application of the Mask Policy and tag on all columns in Prod Catalog tables with identified sensitive information
# MAGIC
# MAGIC #### Note - you have to use Shared compute mode in order to apply RLS/CLM policies. This limitation will be waived in the future

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window


pii_results = spark.table(f"{diz_catalog}.{diz_schema}.pii_results")
max_ts = pii_results.agg({"identification_date": "max"}).collect()[0][0]
pii_results = pii_results.filter(pii_results.identification_date==max_ts)

dataCollect = pii_results.collect()
for row in dataCollect:
  table_name = row['prod_table_name']
  column_name = row['column_name']
  identification_time = row['identification_date']
  print(f"Setting UC tag and column mask for {table_name} in column {column_name}")
  spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET TAGS ('pii')")
  spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET TAGS ('pii_alert' = '{identification_time}')")
  spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET MASK {mask_policy_name}")

# COMMAND ----------


