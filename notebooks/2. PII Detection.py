# Databricks notebook source
# MAGIC %md
# MAGIC # PII detection with DiscoverX & Presidio
# MAGIC
# MAGIC This notebooks uses [DiscoverX](https://github.com/databrickslabs/discoverx) to run PII detection with [Presidio](https://microsoft.github.io/presidio/) over a set of tables in Unity Catalog.
# MAGIC
# MAGIC The notebook will:
# MAGIC 1. Use DiscoverX to sample records from a set of tables from Unity Catalog and unpivot all string columns into a long format dataset
# MAGIC 2. Run PII detection with Presidio
# MAGIC 3. Compute summarised statistics per table and column and save them in scanning results table
# MAGIC 4. Create tags in UC for Columns with high probablity of containing sensitive data

# COMMAND ----------

# Set up the Notebook Widgets
dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");

dbutils.widgets.text("dev_catalog", "dss_demo_dev", "Target Dev Catalog:");
dbutils.widgets.text("prod_catalog", "dss_demo_prod", "Target Prod Catalog:");
dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install dependencies

# COMMAND ----------

# Install required Python libs
%pip install presidio_analyzer==2.2.33 dbl-discoverx==0.0.5 presidio_anonymizer

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download detection model

# COMMAND ----------

# MAGIC %sh python -m spacy download en_core_web_lg

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define variables

# COMMAND ----------

diz_catalog = dbutils.widgets.get("diz_catalog")
diz_schema = dbutils.widgets.get("diz_schema")

# Selecting just Bronze tables containing raw data
from_tables = f"{diz_catalog}.{diz_schema}.*bronze*"

# Setting analyzer sample size for PII scanning
sample_size = 500

# COMMAND ----------

import pandas as pd
import pickle
from presidio_analyzer import AnalyzerEngine, PatternRecognizer
from pyspark.sql.functions import pandas_udf, col, concat, lit, explode, count, avg, min, max, sum
from pyspark.sql.types import ArrayType, StringType, StructType, FloatType, StructField
from typing import Iterator


# COMMAND ----------

from discoverx import DX

dx = DX()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform all sampled tables to long format

# COMMAND ----------

# This will take all columns of type string and unpivot (melt) them into a long format dataset
unpivoted_df = (
    dx.from_tables(from_tables)
    .unpivot_string_columns(sample_size=sample_size)
    .to_union_dataframe()
    .localCheckpoint()  # Checkpointing to reduce the query plan size
)

unpivoted_df.display()

# COMMAND ----------

unpivoted_stats = unpivoted_df.groupBy("table_catalog", "table_schema", "table_name", "column_name").agg(
    count("string_value").alias("sampled_rows_count")
)

unpivoted_stats.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Presidio UDFs
# MAGIC

# COMMAND ----------


# Define the analyzer, and add custom matchers if needed
analyzer = AnalyzerEngine()

# broadcast the engines to the cluster nodes
broadcasted_analyzer = sc.broadcast(analyzer)


# define a pandas UDF function and a series function over it.
def analyze_text(text: str, analyzer: AnalyzerEngine) -> list[str]:
    try:
        analyzer_results = analyzer.analyze(text=text, language="en")
        dic = {}
        # Deduplicate the detections and take the max score per entity type
        for r in analyzer_results:
            if r.entity_type in dic.keys():
                dic[r.entity_type] = max(r.score, dic[r.entity_type])
            else:
                dic[r.entity_type] = r.score
        return [{"entity_type": k, "score": dic[k]} for k in dic.keys()]
    except:
        return []


# define the iterator of series to minimize
def analyze_series(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    analyzer = broadcasted_analyzer.value
    for series in iterator:
        # Use that state for whole iterator.
        yield series.apply(lambda t: analyze_text(t, analyzer))


# define a the function as pandas UDF
analyze = pandas_udf(
    analyze_series,
    returnType=ArrayType(StructType([StructField("entity_type", StringType()), StructField("score", FloatType())])),
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Run PII detections

# COMMAND ----------

detections = (
    unpivoted_df.withColumn(
        "text", concat(col("table_name"), lit(" "), col("column_name"), lit(": "), col("string_value"))
    )
    .withColumn("detection", explode(analyze(col("text"))))
    .select("table_catalog", "table_schema", "table_name", "column_name", "string_value", "detection.*")
)

detections.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####
# MAGIC Compute summarised statistics

# COMMAND ----------

prod_catalog = dbutils.widgets.get("prod_catalog")
dev_catalog = dbutils.widgets.get("dev_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

summarised_detections = (
    detections.groupBy("table_catalog", "table_schema", "table_name", "column_name", "entity_type")
    .agg(count("string_value").alias("value_count"), max("score").alias("max_score"), sum("score").alias("sum_score"))
    .join(unpivoted_stats, ["table_catalog", "table_schema", "table_name", "column_name"])
    .withColumn("score", col("sum_score") / col("sampled_rows_count"))
    .withColumn("full_table_name", concat_ws('.',col("table_catalog"),col("table_schema"),col("table_name")))
    .withColumn("silver_table_name", regexp_replace('table_name', 'bronze', 'silver'))
    .withColumn("prod_table_name", concat(lit(f"{prod_catalog}."), lit(f"{target_schema}."), col("silver_table_name")))
    .withColumn("dev_table_name", concat(lit(f"{dev_catalog}."), lit(f"{target_schema}."), col("silver_table_name")))
    .withColumn("identification_date", current_timestamp())
    .select("table_catalog", "table_schema", "table_name", "column_name", "entity_type", "score", "max_score", "full_table_name", "prod_table_name", "dev_table_name", "identification_date")
)

summarised_detections.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filtering on the PII entity types and setting sensivity level for classification engine

# COMMAND ----------

# Selection of the allowed entity types with highest weighted score

allowed_entity_types = ['PERSON', 
                        'EMAIL_ADDRESS', 
                        'CREDIT_CARD', 
                        'IBAN_CODE', 
                        'IP_ADDRESS', 
                        'PERSON', 
                        'PHONE_NUMBER', 
                        'US_SSN']

w=Window().partitionBy(concat("table_name", "column_name")).orderBy(desc("score"))

summarised_detections_ranked = (summarised_detections
                                .filter(col("entity_type").isin(allowed_entity_types))
                                .withColumn("rank", row_number().over(w))
                                .filter(col("rank")==1)
                                .filter(col("score")>0.5) # false positives sensitivity
                                .drop(col("rank"))
                                )

summarised_detections_ranked.display()
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Assigning tags in DIZ Catalog for better transparency

# COMMAND ----------

dataCollect = summarised_detections_ranked.collect()
for row in dataCollect:
  table_name = row['full_table_name']
  column_name = row['column_name']
  identification_time = row['identification_date']
  print(f"Setting PII tag for {table_name} in column {column_name}")
  spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET TAGS ('pii_alert' = '{identification_time}')")


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Saving PII scan results for audit purposes

# COMMAND ----------

summarised_detections_ranked.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{diz_catalog}.{diz_schema}.pii_results")

# COMMAND ----------


