# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/etl.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/manufacturing/lakehouse-iot-turbine/lakehouse-manuf-iot-turbine-spark-1.png" />

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
# MAGIC ## Databricks has native capabilities to read a multitude of formats: delta, csv, xml, json, avro, parquet, etc etc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Explore the data available in the Catalog Explorer on the left

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/in_notebook_explorer.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read a csv file

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Read raw data**  
# MAGIC    Load IoT CSV files stored in a **Unity Catalog Volume** into a Spark DataFrame.  
# MAGIC
# MAGIC 2. **Explore the dataset**  
# MAGIC    Use the following actions to explore the data:  
# MAGIC    - `.count()` → check the number of records.  
# MAGIC    - `.display()` → preview and visually inspect the data.  
# MAGIC
# MAGIC 3. **Load data with headers**  
# MAGIC    Re-read the CSV specifying `header=True` to correctly interpret column names.  
# MAGIC
# MAGIC 4. **Write data to the Bronze layer**  
# MAGIC    Save the DataFrame as a **Delta table** named `sensor_bronze` within your schema.  
# MAGIC    Use the **overwrite** mode to refresh data and the **overwriteSchema** option to align schema definitions.  
# MAGIC
# MAGIC 5. **Result**  
# MAGIC    The raw CSV data is now stored in a **Bronze Delta table**, representing the first step in the **Medallion Architecture**:  
# MAGIC    **Raw → Bronze → Silver → Gold**

# COMMAND ----------

df_iot = spark.read.csv('/Volumes/dit_dicox_academy-lab/daia2/raw/morning/incoming_data/')

# COMMAND ----------

df_iot.count()

# COMMAND ----------

df_iot.display()

# COMMAND ----------

df_iot = spark.read.option("header", "true").csv('/Volumes/dit_dicox_academy-lab/daia2/raw/morning/incoming_data/')
df_iot.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable(f'`dit_dicox_academy-lab`.{current_user}_schema.sensor_bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read a json file instead

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Read raw text data**  
# MAGIC    - Load the JSON files as text from the **Unity Catalog Volume** to inspect their raw structure.  
# MAGIC    - Use `.display()` to preview the data and `.printSchema()` to understand the inferred schema.
# MAGIC
# MAGIC 2. **Read structured JSON data**  
# MAGIC    - Use `spark.read.json()` to automatically parse the JSON structure into a Spark DataFrame.  
# MAGIC    - Explore the dataset again using `.display()` and `.printSchema()` to confirm schema correctness.
# MAGIC
# MAGIC 3. **Flatten nested structures**  
# MAGIC    - Import PySpark SQL functions and use `F.explode("sensors")` to **flatten the nested JSON array** into individual rows, making the data easier to query and analyze.
# MAGIC
# MAGIC 4. **Inspect transformed data**  
# MAGIC    - Display and print the schema of the exploded DataFrame to verify the transformation results.
# MAGIC
# MAGIC 5. **Write to the Bronze layer**  
# MAGIC    - Save the flattened DataFrame as a **Delta table** (`parts`) under your user schema.  
# MAGIC    - Use `mode("overwrite")` and `option("mergeSchema", "true")` to ensure schema updates are safely applied.
# MAGIC
# MAGIC 6. **Result**  
# MAGIC    - The raw JSON data has been parsed, flattened, and stored as a **Bronze Delta table**, ready for further processing and enrichment in the **Medallion Architecture** pipeline.

# COMMAND ----------

df_text = spark.read.text('/Volumes/dit_dicox_academy-lab/daia2/raw/morning/parts/')

# COMMAND ----------

df_text.display()

# COMMAND ----------

df_text.printSchema()

# COMMAND ----------

df_json = spark.read.json('/Volumes/dit_dicox_academy-lab/daia2/raw/morning/parts/')

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_json = df_json.withColumn("sensors", F.explode("sensors"))

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

df_json.write.mode('overwrite').option("mergeSchema", "true").saveAsTable(f'`dit_dicox_academy-lab`.{current_user}_schema.parts')

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
# MAGIC Link to download if you don't have an xlsx file on your laptop: https://file-examples.com/wp-content/storage/2017/02/file_example_XLS_5000.xls

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/upload.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Install dependencies**  
# MAGIC    - Install the `xlrd` library, which is required by Pandas to read `.xls` Excel files.
# MAGIC
# MAGIC 2. **Read Excel data using Pandas**  
# MAGIC    - Import the `read_excel` function from Pandas.  
# MAGIC    - Define the Excel file path stored in a **Unity Catalog Volume** and specify the target sheet name.  
# MAGIC    - Load the Excel content into a Pandas DataFrame (`df_pandas`).
# MAGIC
# MAGIC 3. **Inspect the data**  
# MAGIC    - Use `.head()` to preview the first 5 rows and confirm column headers.
# MAGIC
# MAGIC 4. **Convert to Spark DataFrame**  
# MAGIC    - Transform the Pandas DataFrame into a Spark DataFrame (`df_spark`) using `spark.createDataFrame()`.  
# MAGIC    - Validate the conversion by checking record count with `.count()`.
# MAGIC
# MAGIC 5. **Clean and rename columns**  
# MAGIC    - Rename undesired auto-generated columns (e.g., `'Unnamed: 0'`) to a more meaningful name such as `'incremental_id'`.
# MAGIC
# MAGIC 6. **Visualize the Bronze DataFrame**  
# MAGIC    - Display the cleaned DataFrame using `.display()` to ensure proper structure and content.
# MAGIC
# MAGIC 7. **Result**  
# MAGIC    - The Excel file is successfully loaded, cleaned, and prepared as a **Bronze-level dataset** — ready for transformation and enrichment in subsequent stages of the **Medallion Architecture** pipeline.

# COMMAND ----------

!pip install xlrd

# COMMAND ----------

from pandas import read_excel

my_sheet = 'Sheet1' # change it to your sheet name, you can find your sheet name at the bottom left of your excel file
file_name = f'/Volumes/dit_dicox_academy-lab/{current_user}_schema/raw/file_example_XLS_5000.xls' # change it to the name of your excel file
df_pandas = read_excel(file_name, sheet_name = my_sheet)

# COMMAND ----------

df_pandas.head() # shows headers with top 5 rows

# COMMAND ----------

df_spark = spark.createDataFrame(df_pandas)

# COMMAND ----------

df_spark.count()

# COMMAND ----------

df_bronze = df_spark.withColumnRenamed('Unnamed: 0', 'incremental_id')

# COMMAND ----------

df_bronze.display()
