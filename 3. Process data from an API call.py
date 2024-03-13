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
# MAGIC ## In Databricks you can run any custom python code. This includes code to read from external sources such as an API

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## I can read from a REST API with the requests library

# COMMAND ----------

url = requests.get('https://raw.githubusercontent.com/IBM/iot-predictive-analytics/master/data/iot_sensor_dataset.csv').content

# COMMAND ----------

raw_data = pd.read_csv(io.StringIO(url.decode('utf-8')))

# COMMAND ----------

df_spark_raw_data = spark.createDataFrame(raw_data)

# COMMAND ----------

df_spark_raw_data.count()

# COMMAND ----------

df_spark_raw_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## There is a python library that allows to read trading data from Stooq.com
# MAGIC
# MAGIC ```
# MAGIC Returns DataFrame/dict of Dataframes of historical stock prices from symbols, over date range, start to end.
# MAGIC ```

# COMMAND ----------

!pip install pandas_datareader

# COMMAND ----------

import pandas_datareader as pdr
import pyspark.pandas as ps
import requests
import pandas as pd
import io

# COMMAND ----------


dfr = pdr.stooq.StooqDailyReader(
    symbols=['ENI.DE'],
    start='1/1/15',
    end='2/29/24'
)

df_bronze = spark.createDataFrame(dfr.read().reset_index())

# COMMAND ----------

df_bronze.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.bronze_layer')

# COMMAND ----------

df_bronze = spark.read.table(f'{current_user}_catalog.default.bronze_layer')

# COMMAND ----------

df_silver_one = df_bronze\
        .withColumnRenamed("('Date', '')", "date")\
        .withColumnRenamed("('Close', 'ENI.DE')", "close")\
        .withColumnRenamed("('High', 'ENI.DE')", "high")\
        .withColumnRenamed("('Low', 'ENI.DE')", "low")\
        .withColumnRenamed("('Open', 'ENI.DE')", "open")\
        .withColumnRenamed("('Volume', 'ENI.DE')", "volume")

# COMMAND ----------

df_silver_one.display()

# COMMAND ----------

df_silver_one.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.silver_one_layer')

# COMMAND ----------

df_silver_one = spark.read.table(f'{current_user}_catalog.default.silver_one_layer')

# COMMAND ----------

df_silver_two = df_silver_one.withColumn('date', df_silver_one['date'].cast('date'))

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_silver_two.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.silver_two_layer')
