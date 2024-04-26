# Databricks notebook source
# MAGIC %md
# MAGIC ## In this step we'll persist anonimization in Dev environment so there is less risk of sensitive data exposure in a less governed environment
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Step 1: Install the encryption library

# COMMAND ----------


dbutils.widgets.text("diz_catalog", "pii_data", "DIZ Catalog:");
dbutils.widgets.text("diz_schema", "default", "DIZ Schema:");

dbutils.widgets.text("dev_catalog", "dss_demo_dev", "Target Dev Catalog:");
dbutils.widgets.text("target_schema", "default", "Dev/Prod Schema:");
dbutils.widgets.text("free_text", "freetext", "Column with a free text:");

# COMMAND ----------

diz_catalog = dbutils.widgets.get("diz_catalog")
diz_schema = dbutils.widgets.get("diz_schema")

dev_catalog = dbutils.widgets.get("dev_catalog")
target_schema = dbutils.widgets.get("target_schema")
anonymize_column = dbutils.widgets.get("free_text")

# COMMAND ----------

# MAGIC %run ./Anonymization_func

# COMMAND ----------

# MAGIC %pip install ff3 tabulate mimesis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Setup the encryption key and tweak

# COMMAND ----------

import secrets  
  
# If needed generate a 256 bit key, store as a secret...
#key = secrets.token_bytes(32).hex()

# If needed generate a 7 byte tweak, store as a secret...
#tweak = secrets.token_bytes(7).hex()

key = dbutils.secrets.get("encrypt", "fpe_key")
tweak = dbutils.secrets.get("encrypt", "fpe_tweak")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Define the character sets and the expected behavior when we encounter special characters

# COMMAND ----------

# Determine what to do with any special characters in the plaintext...
#   Options are "tokenize", or "reassemble" where:
# 
#     1. "TOKENIZE" -> Tokenize the whole string including special characters with an ASCII charset 
#     2. "REASSEMBLE" -> Try and preserve the format of the input string by removing the special characters, 
#     tokenizing the alphanum characters and then reassembling both afterwards
#
SPECIAL_CHAR_MODE = "REASSEMBLE" 

# Define the character sets...
NUMERIC_CHARSET = "0123456789"
ALPA_CHARSET_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
ALPHA_CHARSET_LOWER = "abcdefghijklmnopqrstuvwxyz"
ALPHA_CHARSET_ALL = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
ALPHANUMERIC_CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
ASCII_CHARSET = """0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ \t\n\r\x0b\x0c"""
SPECIAL_CHARSET = """!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~ """

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Declare some helper functions and our Pandas UDF

# COMMAND ----------

from ff3 import FF3Cipher

# Helper functions
def reassemble_string(string: str, positions: list, characters: str) -> str:

  for i in range(len(positions)):  
    pos = positions[i]   
    char = characters[i]  
    string = string[:pos] + char + string[pos:]
  return string

def encrypt_or_decrypt(text: str, charset: str, operation: str) -> str:

  c = FF3Cipher.withCustomAlphabet(key, tweak, charset)
  split_string = lambda string: (lambda s: s[:-1] + [s[-2] + s[-1]] if len(s[-1]) < 5 else s)([string[i:i+23] for i in range(0, len(string), 23)])

  if len(text) > 28:
    split = split_string(text)
    if operation == "ENCRYPT":
      output = "".join(list(map(lambda x: c.encrypt(x), split)))
    elif operation == "DECRYPT":
      output = "".join(list(map(lambda x: c.decrypt(x), split)))
    else:
      raise NotImplementedError("Invalid option - must be 'ENCRYPT' or 'DECRYPT'")
  elif len(text) < 6:
    longer_text = str(text+text)
    if operation == "ENCRYPT":
      output = c.encrypt(longer_text)
    elif operation == "DECRYPT":
      output = c.decrypt(longer_text)
    else:
      raise NotImplementedError("Invalid option - must be 'ENCRYPT' or 'DECRYPT'")  
  else:
    if operation == "ENCRYPT":
      output = c.encrypt(text)
    elif operation == "DECRYPT":
      output = c.decrypt(text)
    else:
      raise NotImplementedError("Invalid option - must be 'ENCRYPT' or 'DECRYPT'")  
  return output
  
def encrypt_or_decrypt_alpha(text: str, operation: str) -> str:

  if text.isupper():
    return encrypt_or_decrypt(text, ALPA_CHARSET_UPPER, operation) 
  elif text.islower():
    return encrypt_or_decrypt(text, ALPHA_CHARSET_LOWER, operation) 
  else:  
    return encrypt_or_decrypt(text, ALPHA_CHARSET_ALL, operation)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Encryption functions...
def fpe_encrypt_or_decrypt(text: str, operation: str) -> str:

  if len(text) < 6: 
    raise ValueError(f"Input string length {len(text)} is not within minimum bounds: {text}")

  if text.isnumeric():
    return encrypt_or_decrypt(text, NUMERIC_CHARSET, operation)
  
  elif text.isalnum():
    return encrypt_or_decrypt(text, ALPHANUMERIC_CHARSET, operation)
    
  elif text.isalpha():
    return encrypt_or_decrypt_alpha(text, operation)
  
  elif text.isascii():

    import re
    encrypt_or_decrypt_by_type = lambda x, y : encrypt_or_decrypt(x, NUMERIC_CHARSET, y) if x.isnumeric() else encrypt_or_decrypt(x, ALPHANUMERIC_CHARSET, y) if x.isalnum() else encrypt_or_decrypt_alpha(x, y) if x.isalpha() else None 

    if SPECIAL_CHAR_MODE == "TOKENIZE":
      return encrypt_or_decrypt(text, ASCII_CHARSET, operation)  
    elif SPECIAL_CHAR_MODE == "REASSEMBLE":
      extract_special_chars = lambda string: ([char for char in re.findall(r"[^\w]", string)], [i for i, char in enumerate(string) if char in SPECIAL_CHARSET])  
      characters, positions = extract_special_chars(text)
      removed = re.sub("([^a-zA-Z0-9])", "", text)
      encrypted_decrypted = encrypt_or_decrypt_by_type(removed, operation)
      reassembled = reassemble_string(encrypted_decrypted, positions, characters)
      return reassembled
    else:
      raise NotImplementedError("Invalid option - must be 'TOKENIZE' or 'REASSEMBLE'")

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Pyspark Pandas UDFs...
def fpe_encrypt_series(s: pd.Series) -> pd.Series:

  return s.astype(str).apply(lambda x: fpe_encrypt_or_decrypt(x, "ENCRYPT"))

fpe_encrypt_pandas_udf = pandas_udf(fpe_encrypt_series, returnType=StringType())

def fpe_decrypt_series(s: pd.Series) -> pd.Series:

  return s.astype(str).apply(lambda x: fpe_encrypt_or_decrypt(x, "DECRYPT"))

fpe_decrypt_pandas_udf = pandas_udf(fpe_decrypt_series, returnType=StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Encrypt the data with FPE

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType

pii_results = spark.table(f"{diz_catalog}.{diz_schema}.pii_results")
max_ts = pii_results.agg({"identification_date": "max"}).collect()[0][0]
pii_results = pii_results.filter(pii_results.identification_date==max_ts)

pii_columns = pii_results.groupby("full_table_name", "dev_table_name").agg(collect_list('column_name').alias('columns'))
display(pii_columns)

# COMMAND ----------


# Read tables with 'bronze' in the name from the DIZ Catalog
source_tables = spark.sql(f"SHOW TABLES IN {diz_catalog}.{diz_schema} LIKE '*bronze*'")

source_tables = source_tables.withColumn("silver_table_name", regexp_replace('tableName', 'bronze', 'silver'))

# Iterate through each table and copy it to the target Dev Catalog
for table_row in source_tables.collect():
    silver_table_name = f"{dev_catalog}.{target_schema}." + table_row['silver_table_name'];
    diz_table_name = f"{diz_catalog}.{diz_schema}." + table_row['tableName'];
    print("Copying table: " + diz_table_name)
    encrypted_table = spark.table(diz_table_name)

    columns = [x.columns for x in pii_columns.filter(col('full_table_name') == diz_table_name).collect()]
    if columns:
        print("Encrypting table: " + silver_table_name)
        for column in columns[0]: 
            print('Encrypting column: 'f'{column}')
            encrypted_table = encrypted_table.withColumn(f'{column}', fpe_encrypt_pandas_udf(col(f'{column}')))

    columns_freetext = encrypted_table.columns
    if anonymize_column in columns_freetext:
        print("Anonymizing freetext fields in: " + silver_table_name)

        anonymized_df = encrypted_table.withColumn(
            anonymize_column, anonymize(col(anonymize_column))
            )
        anonymized_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(silver_table_name)
    else:
        encrypted_table.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(silver_table_name)
