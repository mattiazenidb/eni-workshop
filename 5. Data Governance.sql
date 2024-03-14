-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/governance.png" />

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Ensuring Governance and security for our IOT platform
-- MAGIC
-- MAGIC Data governance and security is hard when it comes to a complete Data Platform. SQL GRANT on tables isn't enough and security must be enforced for multiple data assets (dashboards, Models, files etc).
-- MAGIC
-- MAGIC To reduce risks and driving innovation, Emily's team needs to:
-- MAGIC
-- MAGIC - Unify all data assets (Tables, Files, ML models, Features, Dashboards, Queries)
-- MAGIC - Onboard data with multiple teams
-- MAGIC - Share & monetize assets with external Organizations
-- MAGIC
-- MAGIC <style>
-- MAGIC .box{
-- MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
-- MAGIC .badge {
-- MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
-- MAGIC .badge_b { 
-- MAGIC   height: 35px}
-- MAGIC </style>
-- MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
-- MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
-- MAGIC   <div style="width:200px; float: left; text-align: center">
-- MAGIC     <div class="box" style="">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team A</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
-- MAGIC         Data Analysts<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
-- MAGIC         Data Scientists<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
-- MAGIC         Data Engineers
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team B</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">...</div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on queries, dashboards</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on tables, columns, rows</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on features, ML models, endpoints, notebooks…</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">Permissions on files, jobs</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   <div class="box" style="width:550px; float: left">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
-- MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />Governance and Security</div>
-- MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
-- MAGIC       <ul style="line-height: 2px;">
-- MAGIC         <li>Central catalog - all data assets</li>
-- MAGIC         <li>Data exploration & discovery to unlock new use-cases</li>
-- MAGIC         <li>Permissions cross-teams</li>
-- MAGIC         <li>Reduce risk with audit logs</li>
-- MAGIC         <li>Measure impact with lineage</li>
-- MAGIC       </ul>
-- MAGIC       + Monetize & Share data with external organization (Delta Sharing)
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.dbutils import DBUtils
-- MAGIC import json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark = SparkSession.getActiveSession()
-- MAGIC dbutils = DBUtils(spark)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC current_user = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())["tags"]["user"].split('@')[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("current_user", current_user)

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

SHOW TABLES IN ${current_user}_catalog.default

-- COMMAND ----------

SHOW GRANT ON ${current_user}_catalog.default.turbine_training_dataset

-- COMMAND ----------

SHOW TABLES IN landing.power

-- COMMAND ----------

SHOW VOLUMES IN landing.power

-- COMMAND ----------

SHOW GRANT ON VOLUME landing.power.turbine_raw_landing

-- COMMAND ----------

-- Let's grant our ANALYSTS a SELECT permission:
-- Note: make sure you created an analysts and dataengineers group first.

GRANT SELECT ON TABLE ${current_user}_catalog.default.turbine_training_dataset TO `account users`;
GRANT USAGE ON CATALOG ${current_user}_catalog TO `account users`;
GRANT USAGE ON SCHEMA ${current_user}_catalog.default TO `account users` ;

-- COMMAND ----------

SHOW GRANT ON ${current_user}_catalog.default.turbine_training_dataset
