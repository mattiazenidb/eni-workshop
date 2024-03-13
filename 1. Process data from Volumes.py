# Databricks notebook source
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
# MAGIC ## Databricks has native capabilities to read a multitude of formats.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explore the data available from the Catalog Explorer on the right

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/in_notebook_explorer.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

df_iot = spark.read.csv('/Volumes/landing/power/readings/turbines')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## With proper libraries it can read any format! For example, Excel files

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

from pandas import read_excel

my_sheet = 'Sheet1' # change it to your sheet name, you can find your sheet name at the bottom left of your excel file
file_name = '/Volumes/odl_user_1256917_catalog/default/odl_user_1256917_volume/file_example_XLS_5000.xls' # change it to the name of your excel file
df_pandas = read_excel(file_name, sheet_name = my_sheet)

# COMMAND ----------

print(df_pandas.head()) # shows headers with top 5 rows

# COMMAND ----------

df_spark = spark.createDataFrame(df)

# COMMAND ----------

df_spark.count()

# COMMAND ----------

df_bronze = df_spark.withColumnRenamed('Unnamed: 0', 'incremental_id')

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

df.write.saveAsTable("main.default.test_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Upload data to a Volume from your local computer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/upload.gif" style="float:right; margin-left: 10px" />
