# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/etl.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-2.png" />

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

df_sensor_bronze = spark.read.table(f'{current_user}_catalog.default.sensor_bronze')

# COMMAND ----------

display(df_sensor_bronze)

# COMMAND ----------

df_sensor_bronze.printSchema()

# COMMAND ----------

import pyspark.sql.functions as F

df_sensor_bronze = df_sensor_bronze.withColumn("timestamp", F.col("timestamp").cast("bigint"))\
                    .withColumn("sensor_a", F.col("sensor_a").cast("double"))\
                    .withColumn("sensor_b", F.col("sensor_b").cast("double"))\
                    .withColumn("sensor_c", F.col("sensor_c").cast("double"))\
                    .withColumn("sensor_d", F.col("sensor_d").cast("double"))\
                    .withColumn("sensor_e", F.col("sensor_e").cast("double"))\
                    .withColumn("sensor_f", F.col("sensor_f").cast("double"))\
                    .withColumn("energy", F.col("energy").cast("double"))

# COMMAND ----------

df_sensor_bronze.printSchema()

# COMMAND ----------

df_sensor_bronze.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.sensor_bronze_intermediate')

# COMMAND ----------

df_sensor_bronze_intermediate = spark.read.table(f'{current_user}_catalog.default.sensor_bronze_intermediate')

# COMMAND ----------

first_turbine = df_sensor_bronze_intermediate.limit(1).collect()[0]['turbine_id']
df = df_sensor_bronze_intermediate.where(f"turbine_id == '{first_turbine}' ").orderBy('timestamp').pandas_api()
df.plot(x="timestamp", y=["sensor_f", "sensor_e"], kind="line")

# COMMAND ----------

import pyspark.sql.functions as F

#Compute std and percentil of our timeserie per hour
sensors = [c for c in df_sensor_bronze_intermediate.columns if "sensor" in c]
aggregations = [F.avg("energy").alias("avg_energy")]

for sensor in sensors:
  aggregations.append(F.stddev_pop(sensor).alias("std_"+sensor))
  aggregations.append(F.percentile_approx(sensor, [0.1, 0.3, 0.6, 0.8, 0.95]).alias("percentiles_"+sensor))
  
df_sensor_hourly = (df_sensor_bronze_intermediate
          .withColumn("hourly_timestamp", F.date_trunc("hour", F.from_unixtime("timestamp")))
          .groupBy('hourly_timestamp', 'turbine_id').agg(*aggregations))

# COMMAND ----------

display(df_sensor_hourly)

# COMMAND ----------

df_sensor_hourly.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.sensor_hourly')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-3.png" />

# COMMAND ----------

df_sensor_hourly = spark.read.table(f'{current_user}_catalog.default.sensor_hourly')
df_turbine = spark.read.table(f'{current_user}_catalog.default.turbine')
df_historical_turbine_status = spark.read.table(f'{current_user}_catalog.default.historical_turbine_status')

# COMMAND ----------

df_turbine_training_dataset = (df_sensor_hourly
  .join(df_turbine, ['turbine_id']).drop("row", "_rescued_data")
  .join(df_historical_turbine_status, ['turbine_id'])
  .drop("_rescued_data"))

# COMMAND ----------

display(df_turbine_training_dataset)

# COMMAND ----------

df_turbine_training_dataset.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'{current_user}_catalog.default.turbine_training_dataset')
