# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Creating Presidio Analyzer and Anonimizer UDFs

# COMMAND ----------

dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");
dbutils.widgets.text("free_text", "freetext", "Column with a free text:");

dbutils.widgets.text("prod_catalog", "dss_demo_prod", "Target Prod Catalog:");
dbutils.widgets.text("dev_catalog", "dss_demo_dev", "Target Dev Catalog:");
dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");

# COMMAND ----------

diz_catalog = dbutils.widgets.get("diz_catalog")
diz_schema = dbutils.widgets.get("diz_schema")
anonymize_column = dbutils.widgets.get("free_text")

prod_catalog = dbutils.widgets.get("prod_catalog")
dev_catalog = dbutils.widgets.get("dev_catalog")
target_schema = dbutils.widgets.get("target_schema")

# COMMAND ----------

# MAGIC %pip install presidio_analyzer presidio_anonymizer

# COMMAND ----------

# MAGIC %sh
# MAGIC python -m spacy download en_core_web_lg

# COMMAND ----------


from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from pyspark.sql.types import StringType
from pyspark.sql.functions import input_file_name, regexp_replace
from pyspark.sql.functions import col, pandas_udf
import pandas as pd
import os

analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

# broadcast the engines to the cluster nodes
broadcasted_analyzer = sc.broadcast(analyzer)
broadcasted_anonymizer = sc.broadcast(anonymizer)

# define a pandas UDF function and a series function over it.
def anonymize_text(text: str) -> str:
    analyzer = broadcasted_analyzer.value
    anonymizer = broadcasted_anonymizer.value
    analyzer_results = analyzer.analyze(text=text, language="en")
    anonymized_results = anonymizer.anonymize(
        text=text,
        analyzer_results=analyzer_results
    )
    return anonymized_results.text


def anonymize_series(s: pd.Series) -> pd.Series:
    return s.astype(str).apply(anonymize_text)

# define a the function as pandas UDF
anonymize = pandas_udf(anonymize_series, returnType=StringType())
