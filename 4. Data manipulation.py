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

# MAGIC %md
# MAGIC ## Data Manipulation in Databricks using SQL and Spark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM emmanuele_bello.default.sensor_bronze

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session if not already existing
spark = SparkSession.builder.getOrCreate()

# Read the table
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Count rows
row_count = df.count()

print(f"Total number of rows: {row_count}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emmanuele_bello.default.sensor_bronze;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session if not already created
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Show the first few rows
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC SELECT - Basic column selection with aliasing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   sensor_A,
# MAGIC   sensor_B,
# MAGIC   energy,
# MAGIC   sensor_A + sensor_B AS total_sensor_ab
# MAGIC FROM emmanuele_bello.default.sensor_bronze;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Select and compute total_sensor_ab
result_df = df.select(
    col("turbine_id"),
    col("sensor_A"),
    col("sensor_B"),
    col("energy"),
    (col("sensor_A") + col("sensor_B")).alias("total_sensor_ab")
)

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC WHERE - Filter high energy readings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   timestamp,
# MAGIC   energy
# MAGIC FROM emmanuele_bello.default.sensor_bronze
# MAGIC WHERE energy < 0.0029 
# MAGIC   AND sensor_A > -1.0

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Apply filtering and select columns
filtered_df = df.filter(
    (col("energy") < 0.0029) & (col("sensor_A") > -1.0)
).select(
    "turbine_id",
    "timestamp",
    "energy"
)

# Show the result
display(filtered_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Subqueries — Turbines with above-average energy

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT turbine_id, energy  FROM emmanuele_bello.default.sensor_bronze
# MAGIC   WHERE energy < (SELECT AVG(energy) as avg_energy FROM emmanuele_bello.default.sensor_bronze) AND turbine_id IN ('6214f4a3-8173-89f4-87fe-683fae6da5b5', '25da23fd-e0e1-2848-f85c-4315228854e6')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Compute average energy (subquery equivalent)
avg_energy = df.select(avg("energy").alias("avg_energy")).collect()[0]["avg_energy"]

# Define the turbine IDs to filter
target_turbines = [
    "6214f4a3-8173-89f4-87fe-683fae6da5b5", 
    "25da23fd-e0e1-2848-f85c-4315228854e6"
]

# Apply filtering and select the result
result_df = df.filter(
    (col("energy") < avg_energy) & 
    (col("turbine_id").isin(target_turbines))
).select("turbine_id", "energy")

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC CTE (Common Table Expression) — Normalize sensor_D readings

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg_sensor_per_turbine AS (
# MAGIC   SELECT 
# MAGIC     turbine_id,
# MAGIC     AVG(sensor_D) AS avg_sensor_D
# MAGIC   FROM emmanuele_bello.default.sensor_bronze
# MAGIC   GROUP BY turbine_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   b.turbine_id,
# MAGIC   b.sensor_D,
# MAGIC   a.avg_sensor_D
# MAGIC FROM emmanuele_bello.default.sensor_bronze b
# MAGIC JOIN avg_sensor_per_turbine a
# MAGIC   ON b.turbine_id = a.turbine_id
# MAGIC ORDER BY b.turbine_id, b.sensor_D DESC;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the base table
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Step 1: Calculate average sensor_D per turbine (CTE equivalent)
avg_df = df.groupBy("turbine_id").agg(
    avg("sensor_D").alias("avg_sensor_D")
)

# Step 2: Join with original table
joined_df = df.join(avg_df, on="turbine_id", how="inner") \
              .select("turbine_id", "sensor_D", "avg_sensor_D") \
              .orderBy("turbine_id", col("sensor_D").desc())

# Show the result
display(joined_df)


# COMMAND ----------

# MAGIC %md
# MAGIC GROUP BY — Average energy per turbine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   ROUND(AVG(energy), 6) AS avg_energy
# MAGIC FROM emmanuele_bello.default.sensor_bronze
# MAGIC GROUP BY turbine_id
# MAGIC ORDER BY avg_energy DESC;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Group by turbine_id and calculate rounded average energy
result_df = df.groupBy("turbine_id") \
    .agg(round(avg("energy"), 6).alias("avg_energy")) \
    .orderBy(col("avg_energy").desc())

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC WINDOWING FUNCTIONS — Energy change over time per turbine
# MAGIC Using LAG, https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/functions/lag

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   timestamp,
# MAGIC   energy,
# MAGIC   LAG(energy) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS previous_energy,
# MAGIC   energy - LAG(energy) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS delta_energy
# MAGIC FROM emmanuele_bello.default.sensor_bronze;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table("emmanuele_bello.default.sensor_bronze")

# Define the window specification
window_spec = Window.partitionBy("turbine_id").orderBy("timestamp")

# Add lag column and delta calculation
result_df = df.withColumn("previous_energy", lag("energy").over(window_spec)) \
              .withColumn("delta_energy", col("energy") - col("previous_energy")) \
              .select("turbine_id", "timestamp", "energy", "previous_energy", "delta_energy")

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC UDF (User-Defined Function) — Categorize energy (SQL VERSION)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION categorize_energy(energy DOUBLE)
# MAGIC RETURNS STRING
# MAGIC RETURN 
# MAGIC   CASE 
# MAGIC     WHEN energy > 0.1 THEN 'High'
# MAGIC     WHEN energy < 0.1 THEN 'Low'
# MAGIC     ELSE 'Normal'
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   energy,
# MAGIC   categorize_energy(energy) AS energy_level
# MAGIC FROM emmanuele_bello.default.sensor_bronze;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC UDF (User-Defined Function) — Categorize sensor_E values (Spark VERSION)

# COMMAND ----------

# Register UDF in PySpark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_energy(value):
    if value > 0.1:
        return "High"
    elif value < 0.1:
        return "Low"
    else:
        return "Normal"

spark.udf.register("categorize_energy", categorize_energy, StringType())

# SQL usage
df_sensor_bronze = spark.sql("""
SELECT 
  turbine_id,
  energy,
  categorize_energy(energy) AS energy_level
FROM emmanuele_bello.default.sensor_bronze
""")

display(df_sensor_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ready for ML Training

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-3.png" />

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM emmanuele_bello.default.turbine_training_dataset
