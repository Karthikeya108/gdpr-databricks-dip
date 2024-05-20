# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Creating Presidio Analyzer and Anonimizer UDFs

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
