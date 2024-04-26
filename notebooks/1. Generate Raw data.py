# Databricks notebook source
# MAGIC %md
# MAGIC # Raw data ingestion into Bronze tables
# MAGIC
# MAGIC For the demo purposes we'll generate 2 tables with sensitive data and save them in our DIZ Catalog, in real world scenario those could be csv files with raw historical data from the new data sources which have been uploaded into Volume of the DIZ catalog by the downstream workload or inserted directly from the source as part of the ingestion pipeline. 
# MAGIC As those sources have been established years ago with poor documentation, no one in the company knows whether any of the tables contain sensible information. As we follow strict security rules in the company, we'd need to discover which column contain PII before uploading them to LoBs Catalog.

# COMMAND ----------

dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");
dbutils.widgets.text("num_rows", "1000", "Number of rows:");

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${diz_catalog};
# MAGIC USE SCHEMA ${diz_schema};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generating Fake PII data

# COMMAND ----------

# MAGIC %pip install -q faker mimesis presidio_analyzer presidio_anonymizer

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import date
import random
from faker import Faker
from mimesis import Generic
from mimesis.locales import Locale

schema = StructType([
  StructField("customer_id", LongType(), False),
  StructField("name", StringType(), False),
  StructField("email", StringType(), False),
  StructField("date_of_birth", DateType(), False),
  StructField("age", LongType(), False),
  StructField("address", StringType(), False),
  StructField("ipv4", StringType(), False),
  StructField("ipv6", StringType(), False),
  StructField("mac_address", StringType(), False),
  StructField("phone_number", StringType(), False),
  StructField("ssn", StringType(), False),
  StructField("iban", StringType(), False),
  StructField("credit_card", LongType(), False),
  StructField("expiry_date", StringType(), False),
  StructField("security_code", StringType(), False),
  StructField("freetext", StringType(), False)
  ])

fake = Faker('en_US')
generic = Generic(locale=Locale.EN)

def get_random_pii():
  return random.choice([fake.ascii_free_email(), fake.ipv4(), fake.ipv6()])

@pandas_udf("long")
def get_customer_id(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  for id in batch_iter:
      yield int(time.time()) + id

def generate_fake_data(pdf: pd.DataFrame) -> pd.DataFrame:
    
  def generate_data(y):
    
    dob = fake.date_between(start_date='-99y', end_date='-18y')

    y["name"] = fake.name()
    y["email"] = fake.ascii_free_email()
    y["date_of_birth"] = dob #.strftime("%Y-%m-%d")
    y["age"] = date.today().year - dob.year
    y["address"] = fake.address()
    y["ipv4"] = fake.ipv4()
    y["ipv6"] = fake.ipv6()
    y["mac_address"] = fake.mac_address()
    y["phone_number"] = fake.phone_number()
    y["ssn"] = fake.ssn()
    y["iban"] = fake.iban()
    y["credit_card"] = int(fake.credit_card_number())
    y["expiry_date"] = fake.credit_card_expire()
    y["security_code"] = fake.credit_card_security_code()
    y["freetext"] = f"{fake.sentence()} {get_random_pii()} {fake.sentence()} {get_random_pii()} {fake.sentence()}"

    return y
    
  return pdf.apply(generate_data, axis=1).drop(["partition_id", "id"], axis=1)

# COMMAND ----------

def generate_fake_pii_data(num_rows=1000):
  initial_data = spark.range(1, num_rows+1).withColumn("customer_id", get_customer_id(col("id"))) 
  return (
  initial_data
  .withColumn("partition_id", spark_partition_id())
  .groupBy("partition_id")
  .applyInPandas(generate_fake_data, schema)
  .orderBy(asc("customer_id")))

# COMMAND ----------

number_of_rows = int(dbutils.widgets.get("num_rows"))

customers_df = generate_fake_pii_data(num_rows=number_of_rows)
employees_df = generate_fake_pii_data(num_rows=number_of_rows)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Raw data as Bronze tables in the Target Schema

# COMMAND ----------

customers_df.write.mode("overwrite").saveAsTable("bronze_customers")
employees_df.write.mode("overwrite").saveAsTable("bronze_employees")