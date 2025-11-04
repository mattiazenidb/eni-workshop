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

# Get current user (email format: user@domain.com)
user_email = spark.sql("SELECT current_user()").collect()[0][0]
# Optional: sanitize for catalog/schema naming conventions
#current_user = user_email.split("@")[0].replace('.', '_').replace('-', '_')
current_user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']['user'].split('@')[0].replace('.', '_') 
# Build the full catalog path
catalog_path = f"{current_user}.default.sensor_bronze"
catalog_path_ml = f"{current_user}.default.turbine_training_dataset"
dbutils.widgets.text("catalog_path", catalog_path)
dbutils.widgets.text("catalog_path_ml", catalog_path_ml)
display(current_user)


# COMMAND ----------

# MAGIC %md
# MAGIC # 📘 Data Manipulation in Databricks using SQL and Spark
# MAGIC
# MAGIC Welcome to this notebook on **data manipulation** in **Databricks**, where we explore how to transform, filter, and manage data using both **SQL** and **Apache Spark (PySpark)**.
# MAGIC
# MAGIC This guide covers practical examples and best practices for working with structured data, using the full potential of the **Databricks Lakehouse Platform**.
# MAGIC
# MAGIC ## 📌 Objectives
# MAGIC By the end of this notebook, you will learn how to:
# MAGIC - Query and manipulate data using Databricks SQL
# MAGIC - Perform transformations using Spark DataFrames
# MAGIC - Apply filtering, joins, and aggregations efficiently
# MAGIC - Understand how SQL and Spark APIs can complement each other
# MAGIC
# MAGIC > 🛠️ This notebook is intended for data engineers, analysts, and data scientists working with large-scale datasets on Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM ${catalog_path}

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session if not already existing
spark = SparkSession.builder.getOrCreate()

# Read the table
df = spark.table(f'{current_user}.default.sensor_bronze')

# Count rows
row_count = df.count()

print(f"Total number of rows: {row_count}")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog_path};
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session if not already created
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table(f'{current_user}.default.sensor_bronze')

# Show the first few rows
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT – Basic Column Selection with Aliasing
# MAGIC
# MAGIC In both **SQL** and **Spark (PySpark)**, selecting specific columns from a dataset is one of the most fundamental operations in data manipulation. Often, you may also want to rename (or alias) columns to make your results clearer, especially when preparing data for reporting or downstream transformations. 
# MAGIC
# MAGIC This section demonstrates how to perform basic column selection and apply aliases using both SQL syntax and PySpark methods, helping you gain flexibility across declarative and programmatic approaches in Databricks.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   sensor_A,
# MAGIC   sensor_B,
# MAGIC   energy,
# MAGIC   sensor_A + sensor_B AS total_sensor_ab
# MAGIC FROM ${catalog_path};
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table(f'{current_user}.default.sensor_bronze')

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
# MAGIC ### WHERE – Filter High Energy Readings
# MAGIC
# MAGIC Filtering data is a crucial step in any data analysis workflow. The `WHERE` clause in SQL, and its equivalent filtering methods in PySpark, allow you to extract only the rows that meet specific conditions.
# MAGIC
# MAGIC This technique is essential for narrowing down your analysis to the most relevant or critical data points, and can be used for anomaly detection, monitoring, and reporting in energy-related datasets.
# MAGIC
# MAGIC We’ll explore how to apply these filters using both SQL and PySpark within the Databricks environment.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   timestamp,
# MAGIC   energy
# MAGIC FROM ${catalog_path}
# MAGIC WHERE energy < 0.0029 
# MAGIC   AND sensor_A > -1.0

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table into a DataFrame
df = spark.table(f'{current_user}.default.sensor_bronze')

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
# MAGIC ### Subqueries — Turbines with Above-Average Energy
# MAGIC
# MAGIC Subqueries, also known as nested queries, are essential for writing expressive and flexible SQL or Spark transformations. They allow a query to reference the result of another query, enabling more complex filtering, aggregation, and comparison logic.
# MAGIC
# MAGIC This technique is particularly useful when you need to compare individual rows to a derived set of values — such as averages, thresholds, or dynamically computed metrics — without hardcoding those values into your logic.
# MAGIC
# MAGIC In Databricks, subqueries can be written using standard SQL syntax or expressed programmatically using PySpark APIs, offering a scalable and modular approach to data analysis.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT turbine_id, energy  FROM ${catalog_path}
# MAGIC   WHERE energy < (SELECT AVG(energy) as avg_energy FROM ${catalog_path}) AND turbine_id IN ('6214f4a3-8173-89f4-87fe-683fae6da5b5', '25da23fd-e0e1-2848-f85c-4315228854e6')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table(f'{current_user}.default.sensor_bronze')

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
# MAGIC ### CTE (Common Table Expression) — Normalize sensor_D Readings
# MAGIC
# MAGIC Common Table Expressions (CTEs) are an essential tool in both SQL and Spark for structuring complex queries. They allow you to define a temporary, named result set within a query, improving readability and maintainability by breaking down logic into smaller, understandable steps.
# MAGIC
# MAGIC In **SQL**, CTEs are defined using the `WITH` clause and are especially helpful when you need to reference intermediate results multiple times within a query.
# MAGIC
# MAGIC In **Spark (PySpark SQL)**, while there is no native CTE syntax, similar functionality can be achieved using temporary views or chaining transformations on DataFrames. This enables you to modularize logic and reuse intermediate computations cleanly within your Spark pipelines.
# MAGIC
# MAGIC CTEs are particularly useful for preprocessing steps like normalization, filtering, and aggregations before applying final logic or transformations.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH avg_sensor_per_turbine AS (
# MAGIC   SELECT 
# MAGIC     turbine_id,
# MAGIC     AVG(sensor_D) AS avg_sensor_D
# MAGIC   FROM ${catalog_path}
# MAGIC   GROUP BY turbine_id
# MAGIC )
# MAGIC SELECT 
# MAGIC   b.turbine_id,
# MAGIC   b.sensor_D,
# MAGIC   a.avg_sensor_D
# MAGIC FROM ${catalog_path} b
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
df = spark.table(f'{current_user}.default.sensor_bronze')

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
# MAGIC ### GROUP BY — Average Energy per Turbine
# MAGIC
# MAGIC The `GROUP BY` clause is a powerful feature in both SQL and Spark that enables aggregation of data based on one or more categorical columns. It is commonly used to compute summary statistics — such as counts, averages, or totals — for each group of values.
# MAGIC
# MAGIC In **SQL**, `GROUP BY` is used with aggregate functions like `AVG()`, `SUM()`, or `COUNT()` to analyze data trends or performance across distinct categories, such as turbines, sensors, or locations.
# MAGIC
# MAGIC In **Spark**, similar grouping and aggregation can be performed using the `groupBy()` method on a DataFrame, followed by aggregation functions from the PySpark API. This approach is efficient and scalable for large datasets in distributed environments.
# MAGIC
# MAGIC Grouping is essential in scenarios where insights need to be derived per entity — such as calculating the average energy output per turbine — enabling data-driven decision making.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   ROUND(AVG(energy), 6) AS avg_energy
# MAGIC FROM ${catalog_path}
# MAGIC GROUP BY turbine_id
# MAGIC ORDER BY avg_energy DESC;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table(f'{current_user}.default.sensor_bronze')

# Group by turbine_id and calculate rounded average energy
result_df = df.groupBy("turbine_id") \
    .agg(round(avg("energy"), 6).alias("avg_energy")) \
    .orderBy(col("avg_energy").desc())

# Show the result
display(result_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOWING FUNCTIONS — Energy Change Over Time per Turbine
# MAGIC
# MAGIC Window functions enable advanced analytics across a set of rows related to the current row, without aggregating the result set. This is especially powerful for time-series analysis, where tracking changes or trends is essential.
# MAGIC
# MAGIC In **SQL**, functions like `LAG()` allow you to compare the current row’s value with a previous one within a defined partition (e.g., per turbine), making it possible to calculate changes in energy over time.
# MAGIC
# MAGIC In **Spark**, similar behavior can be achieved using the `Window` specification in PySpark. Functions like `lag()` can be used to access prior values within a partition, supporting row-wise comparisons across time or sequence.
# MAGIC
# MAGIC For more details on the `LAG` function and other window functions in Databricks SQL, refer to the documentation:  
# MAGIC 👉 [Databricks SQL: LAG function](https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/functions/lag)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   turbine_id,
# MAGIC   timestamp,
# MAGIC   energy,
# MAGIC   LAG(energy) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS previous_energy,
# MAGIC   energy - LAG(energy) OVER (PARTITION BY turbine_id ORDER BY timestamp) AS delta_energy
# MAGIC FROM ${catalog_path};
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Load the table
df = spark.table(f'{current_user}.default.sensor_bronze')

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
# MAGIC ### UDF (User-Defined Function) — Categorize Energy (SQL Version)
# MAGIC
# MAGIC User-Defined Functions (UDFs) allow you to extend the capabilities of Databricks SQL by embedding custom logic that isn’t available through built-in functions. This is particularly useful when classifying or transforming data in ways specific to your business rules.
# MAGIC
# MAGIC In SQL, you can register and call a UDF to categorize energy readings into buckets (e.g., low, medium, high) based on thresholds. Once defined, the UDF behaves like any other SQL function, enabling reuse across queries and simplifying complex conditional logic.
# MAGIC
# MAGIC Keep in mind that UDFs in SQL may have performance trade-offs compared to built-in functions, so they should be used when necessary logic cannot be expressed otherwise.
# MAGIC

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
# MAGIC FROM ${catalog_path};
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF (User-Defined Function) — Categorize sensor_E values (Spark Version)
# MAGIC
# MAGIC In Spark, User-Defined Functions (UDFs) provide a powerful way to embed custom transformation logic into your DataFrame operations. This is especially useful when the available built-in functions are not sufficient to express the required computation or classification.
# MAGIC
# MAGIC By registering a Python function as a Spark UDF, you can apply complex logic — such as categorizing sensor_E values into predefined buckets (e.g., low, normal, high) — directly within your transformations. Spark UDFs integrate with `select()`, `withColumn()`, and other DataFrame APIs, offering flexible and expressive data manipulation.
# MAGIC
# MAGIC While UDFs add great versatility, keep in mind they may introduce some performance overhead, especially compared to native Spark functions. Use them when custom logic is essential and not easily replicated with existing operations.
# MAGIC

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
# MAGIC ## ✅ Wrapping Up SQL & Spark Transformations
# MAGIC
# MAGIC We’ve now completed a comprehensive round of data transformations using SQL and Spark — preparing our dataset for advanced consumption. In this notebook, we:
# MAGIC
# MAGIC - Selected and renamed columns for improved readability
# MAGIC - Applied filters to isolate high-value records
# MAGIC - Used subqueries and CTEs to simplify complex logic
# MAGIC - Aggregated data to compute meaningful metrics
# MAGIC - Applied window functions to track changes over time
# MAGIC - Created UDFs to enhance feature semantics
# MAGIC
# MAGIC These transformations have cleaned, normalized, and enriched the dataset, making it ready for a wide range of downstream use cases.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Ready for the Gold Layer: ML, BI, and Advanced Analytics
# MAGIC
# MAGIC With a curated and structured dataset in hand, we can now transition toward the **Gold layer** — where insights are generated and value is delivered. This next phase could include:
# MAGIC
# MAGIC - Training predictive models (ML/AI)
# MAGIC - Building BI dashboards and reports
# MAGIC - Performing time series or advanced statistical analysis
# MAGIC - Enabling data products for internal or customer-facing applications
# MAGIC
# MAGIC Let’s move forward and unlock the full potential of our data!
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-3.png" />

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${catalog_path_ml}
