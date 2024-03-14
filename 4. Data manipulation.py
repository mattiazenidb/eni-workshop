# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/etl.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-full.png" />

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
# MAGIC ## Now that we have ingested all the data from the different sources we should enrich/aggregate

# COMMAND ----------

df_sensor_bronze = spark.read.table(f'{current_user}_catalog.default.sensor_bronze')
df_turbine = spark.read.table(f'{current_user}_catalog.default.turbine')
df_historical_turbine_status = spark.read.table(f'{current_user}_catalog.default.historical_turbine_status')
