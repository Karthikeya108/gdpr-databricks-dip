-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
-- MAGIC dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");
-- MAGIC
-- MAGIC dbutils.widgets.text("dev_catalog", "dss_demo_dev", "Target Dev Catalog:");
-- MAGIC dbutils.widgets.text("prod_catalog", "dss_demo_prod", "Target Prod Catalog:");
-- MAGIC dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC diz_catalog = dbutils.widgets.get("diz_catalog")
-- MAGIC diz_schema = dbutils.widgets.get("diz_schema")
-- MAGIC prod_catalog = dbutils.widgets.get("prod_catalog")
-- MAGIC dev_catalog = dbutils.widgets.get("dev_catalog")
-- MAGIC target_schema = dbutils.widgets.get("target_schema")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC source_tables = spark.sql(f"SHOW TABLES IN {diz_catalog}.{diz_schema} LIKE '*bronze*'")
-- MAGIC
-- MAGIC source_tables = source_tables.withColumn("silver_table_name", regexp_replace('tableName', 'bronze', 'silver'))
-- MAGIC
-- MAGIC # Iterate through each table and copy it to the target Prod Catalog
-- MAGIC for table_row in source_tables.collect():
-- MAGIC     prod_table_name = f"{prod_catalog}.{target_schema}." + table_row['silver_table_name'];
-- MAGIC     dev_table_name = f"{dev_catalog}.{target_schema}." + table_row['silver_table_name'];
-- MAGIC     diz_table_name = f"{diz_catalog}.{diz_schema}." + table_row['tableName'];
-- MAGIC     spark.sql(f"DROP TABLE IF EXISTS {prod_table_name}")
-- MAGIC     spark.sql(f"DROP TABLE IF EXISTS {dev_table_name}")
-- MAGIC     spark.sql(f"DROP TABLE IF EXISTS {diz_table_name}")
-- MAGIC

-- COMMAND ----------


