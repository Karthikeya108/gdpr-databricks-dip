# Databricks notebook source
# MAGIC %md
# MAGIC # Publishing cleaned and transformed Bronze tables into Production Catalog 
# MAGIC
# MAGIC >For demo purposes only, there are no real data transformations applied below.
# MAGIC
# MAGIC >Please note that Shared Compute should be used to run this notebook due to RLS/CLM limitation

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Anonymization UDFs to find and encrypt sensitive information in the free text columns

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %run ./Anonymization_func

# COMMAND ----------


dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");

dbutils.widgets.text("prod_catalog", "dss_demo_prod", "Target Prod Catalog:");
dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");
dbutils.widgets.text("free_text", "freetext", "Column with a free text:");

# COMMAND ----------

diz_catalog = dbutils.widgets.get("diz_catalog")
diz_schema = dbutils.widgets.get("diz_schema")

prod_catalog = dbutils.widgets.get("prod_catalog")
target_schema = dbutils.widgets.get("target_schema")
anonymize_column = dbutils.widgets.get("free_text")

# COMMAND ----------

from pyspark.sql.functions import *

# Read tables with 'bronze' in the name from the DIZ Catalog
source_tables = spark.sql(f"SHOW TABLES IN {diz_catalog}.{diz_schema} LIKE '*bronze*'")

source_tables = source_tables.withColumn("silver_table_name", regexp_replace('tableName', 'bronze', 'silver'))

# Iterate through each table and copy it to the target Prod Catalog
for table_row in source_tables.collect():
    silver_table_name = f"{prod_catalog}.{target_schema}." + table_row['silver_table_name'];
    diz_table_name = f"{diz_catalog}.{diz_schema}." + table_row['tableName'];

    spark.sql(f"DROP TABLE IF EXISTS {silver_table_name}") # dropping the table for demo purposes to clean it out if left
    columns = spark.table(diz_table_name).columns

    if anonymize_column in columns:
        print("Anonymizing table: " + silver_table_name)

        anonymized_df = spark.table(diz_table_name).withColumn(
            anonymize_column, anonymize(col(anonymize_column))
            )
        anonymized_df.write.mode("overwrite").saveAsTable(silver_table_name)
    else:
        spark.table(diz_table_name).write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(silver_table_name)
