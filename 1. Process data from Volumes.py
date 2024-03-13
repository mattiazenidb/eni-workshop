# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC Link to download if you don't have an xlsx file on your laptop: https://file-examples.com/wp-content/storage/2017/02/file_example_XLS_5000.xls

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Upload data do a Volume from your local computer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/upload.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

from pandas import read_excel

my_sheet = 'Sheet1' # change it to your sheet name, you can find your sheet name at the bottom left of your excel file
file_name = '/Volumes/odl_user_1256917_catalog/default/odl_user_1256917_volume/file_example_XLS_5000.xls' # change it to the name of your excel file
df = read_excel(file_name, sheet_name = my_sheet)
print(df.head()) # shows headers with top 5 rows

# COMMAND ----------

df_spark = spark.createDataFrame(df)

# COMMAND ----------

df_spark.count()

# COMMAND ----------

df_bronze = df_spark.withColumnRenamed('Unnamed: 0', 'incremental_id')

# COMMAND ----------

df_bronze.display()
