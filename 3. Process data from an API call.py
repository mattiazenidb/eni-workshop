# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/etl.png" />

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
import json

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

# COMMAND ----------

current_user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().safeToJson())['attributes']['user'].split('@')[0].replace('.', '_')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## In Databricks you can run any custom python code. This includes code to read from external sources such as an API

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## I can read from a REST API with the requests library

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Import required libraries**  
# MAGIC    - Import the `requests`, `pandas`, and `io` libraries to handle HTTP requests, data manipulation, and in-memory stream conversion.
# MAGIC
# MAGIC 2. **Retrieve CSV data from a remote source**  
# MAGIC    - Use the `requests.get()` function to download a CSV file directly from a **GitHub repository**.  
# MAGIC    - The dataset used here (`turbine.csv`) contains information about wind turbine locations and metrics.
# MAGIC
# MAGIC 3. **Load the CSV data into Pandas**  
# MAGIC    - Read the downloaded content using `pd.read_csv()` and `io.StringIO()` to create a **Pandas DataFrame** in memory.  
# MAGIC    - This avoids the need to first save the file locally.
# MAGIC
# MAGIC 4. **Convert Pandas DataFrame to Spark DataFrame**  
# MAGIC    - Transform the Pandas DataFrame into a **Spark DataFrame** using `spark.createDataFrame()`.  
# MAGIC    - This step enables distributed processing and scalability within the Databricks environment.
# MAGIC
# MAGIC 5. **Explore the dataset**  
# MAGIC    - Use `.count()` to verify the total number of records (542 rows in this case).  
# MAGIC    - Use `.display()` to visually inspect the dataset and confirm that data types and columns were correctly parsed.
# MAGIC
# MAGIC 6. **Result**  
# MAGIC    - The remote CSV file is successfully fetched, parsed, and converted into a Spark DataFrame, making it ready for downstream processing or integration into the **Bronze layer** of the data pipeline.

# COMMAND ----------

import requests
import pandas as pd
import io

# COMMAND ----------

url = requests.get('https://raw.githubusercontent.com/mattiazenidb/eni-workshop/main/_resources/turbine.csv').content

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
# MAGIC 1. **Filter invalid data**  
# MAGIC    - Apply a filter on the Spark DataFrame to **remove invalid records** where the latitude (`lat`) or longitude (`long`) fields contain the value `'ERROR'`.  
# MAGIC    - This ensures only clean, valid geospatial data is included in the next processing stage.
# MAGIC
# MAGIC 2. **Write the filtered data to an intermediate table**  
# MAGIC    - Save the cleaned dataset as a **Delta table** named `turbine_intermediate` within the user’s schema.  
# MAGIC    - Use the `overwrite` mode to replace any previous version of the table and the `mergeSchema` option to automatically handle schema evolution.
# MAGIC
# MAGIC 3. **Read back the intermediate table**  
# MAGIC    - Load the newly created `turbine_intermediate` Delta table using `spark.read.table()` to validate that the data was correctly written and structured.
# MAGIC
# MAGIC 4. **Result**  
# MAGIC    - The raw turbine dataset has been filtered, cleaned, and stored as an **Intermediate (Silver) table**, representing the next step in the **Medallion Architecture** after the Bronze layer.

# COMMAND ----------

df_spark_raw_data.filter((df_spark_raw_data.lat != 'ERROR') & (df_spark_raw_data.long != 'ERROR')) \
    .write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'`dit_dicox_academy-lab`.{current_user}_schema.turbine_intermediate')

# COMMAND ----------

df_turbine_intermediate = spark.read.table(f'`dit_dicox_academy-lab`.{current_user}_schema.turbine_intermediate')

# COMMAND ----------

df_turbine_intermediate.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Import PySpark functions**  
# MAGIC    - Import the `col` function from `pyspark.sql.functions` to perform column-level transformations on the DataFrame.
# MAGIC
# MAGIC 2. **Convert data types**  
# MAGIC    - Cast the `lat` (latitude) and `long` (longitude) columns from **string** to **double** using `.withColumn()` and `.cast("double")`.  
# MAGIC    - This ensures that these fields are stored as numeric values, enabling accurate geospatial and mathematical operations.
# MAGIC
# MAGIC 3. **Verify schema**  
# MAGIC    - Use `.printSchema()` to confirm that both `lat` and `long` columns have been correctly converted from string to double.  
# MAGIC    - The dataset now has a clean and consistent schema, ready for analytical workloads.
# MAGIC
# MAGIC 4. **Write data to the Silver layer**  
# MAGIC    - Save the transformed dataset as a **Delta table** named `turbine` under the user’s schema.  
# MAGIC    - Use `mode("overwrite")` to replace previous data and `mergeSchema=true` to automatically manage schema updates.
# MAGIC
# MAGIC 5. **Result**  
# MAGIC    - The turbine dataset is now standardized with proper data types and stored in the **Silver layer** of the **Medallion Architecture**, ready for advanced analysis or enrichment in the Gold layer.

# COMMAND ----------

from pyspark.sql.functions import col

df_turbine_intermediate = df_turbine_intermediate.withColumn("lat", col("lat").cast("double"))\
                          .withColumn("long", col("long").cast("double"))

# COMMAND ----------

df_turbine_intermediate.printSchema()

# COMMAND ----------

df_turbine_intermediate.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'`dit_dicox_academy-lab`.{current_user}_schema.turbine')
