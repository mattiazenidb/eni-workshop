# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img width="1000px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/etl.png" />

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import json

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

# COMMAND ----------

current_user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"].split('@')[0]

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC Link to download if you don't have an xlsx file on your laptop: https://file-examples.com/wp-content/storage/2017/02/file_example_XLS_5000.xls

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Databricks has native capabilities to read a multitude of formats: delta, csv, xml, json, avro, parquet, etc etc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explore the data available in the Catalog Explorer on the right

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/in_notebook_explorer.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read a csv file

# COMMAND ----------

df_iot = spark.read.csv('/Volumes/landing/power/readings/turbines')

# COMMAND ----------

df_iot.count()

# COMMAND ----------

df_iot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read a json file instead

# COMMAND ----------

df_text = spark.read.text('/Volumes/landing/power/readings/engines/')

# COMMAND ----------

df_text.display()

# COMMAND ----------

df_text.printSchema()

# COMMAND ----------

df_json = spark.read.json('/Volumes/landing/power/readings/engines/')

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F

df_json = df_json.withColumn("gender", F.when(F.col("gender") == 1, "M").otherwise("F"))

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## With proper libraries you can read any format you like! For example, Excel files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Upload an Excel file to a Volume from your local computer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/upload.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

from pandas import read_excel

my_sheet = 'Sheet1' # change it to your sheet name, you can find your sheet name at the bottom left of your excel file
file_name = f'/Volumes/{current_user}_catalog/default/{current_user}_volume/file_example_XLS_5000.xls' # change it to the name of your excel file
df_pandas = read_excel(file_name, sheet_name = my_sheet)

# COMMAND ----------

df_pandas.head() # shows headers with top 5 rows

# COMMAND ----------

df_spark = spark.createDataFrame(df)

# COMMAND ----------

df_spark.count()

# COMMAND ----------

df_bronze = df_spark.withColumnRenamed('Unnamed: 0', 'incremental_id')

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## The ultimate goal of Data Engineering is to write curated data (cleaned, verified, certified) to tables that others can use

# COMMAND ----------

df_json.write.saveAsTable(f"{current_user}_catalog.default.table_json")
