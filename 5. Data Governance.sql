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

-- MAGIC %md
-- MAGIC ### SHOW CATALOGS — List Available Catalogs
-- MAGIC
-- MAGIC This SQL command is used to display all catalogs accessible in your Databricks environment. 
-- MAGIC
-- MAGIC In the context of Unity Catalog, a **catalog** is the top-level container that organizes schemas (databases), tables, views, and other data objects. Catalogs help manage access control and data governance in a scalable and secure way.
-- MAGIC
-- MAGIC This command is especially useful for:
-- MAGIC - Getting an overview of all available catalogs
-- MAGIC - Validating catalog-level permissions
-- MAGIC - Navigating the data hierarchy before querying
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW CATALOGS;
-- MAGIC

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW TABLES — List Tables in a Schema
-- MAGIC
-- MAGIC This SQL command lists all tables within a specified schema of a catalog. 
-- MAGIC
-- MAGIC In this example, the command is scoped to the `default` schema of the catalog assigned to the current user (e.g., `${current_user}_catalog.default`), which is a common pattern in Databricks environments using Unity Catalog for multi-user isolation.
-- MAGIC
-- MAGIC This is helpful for:
-- MAGIC - Exploring available data assets before querying
-- MAGIC - Verifying the presence of tables after ingestion or transformation
-- MAGIC - Understanding data structure within a specific workspace
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW TABLES IN ${current_user}_catalog.default;
-- MAGIC

-- COMMAND ----------

SHOW TABLES IN ${current_user}_catalog.default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW GRANT — View Permissions on a Table
-- MAGIC
-- MAGIC This SQL command displays the list of privileges (grants) assigned to users, groups, or service principals for a specific table in Unity Catalog.
-- MAGIC
-- MAGIC In this example, we're checking the access controls on the table `turbine_training_dataset` located in the `default` schema of the user-specific catalog `${current_user}_catalog`.
-- MAGIC
-- MAGIC This is useful for:
-- MAGIC - Auditing who has access to specific data assets
-- MAGIC - Verifying permission propagation
-- MAGIC - Ensuring secure access and compliance with data governance policies
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW GRANT ON ${current_user}_catalog.default.turbine_training_dataset;
-- MAGIC

-- COMMAND ----------

SHOW GRANT ON ${current_user}_catalog.default.turbine_training_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW TABLES — List All Tables in a Schema
-- MAGIC
-- MAGIC This SQL command lists all tables available within a specified schema.
-- MAGIC
-- MAGIC In this example, we are listing all the tables contained in the `power` schema of the `landing` catalog.
-- MAGIC
-- MAGIC This is helpful for:
-- MAGIC - Exploring available datasets in a specific namespace
-- MAGIC - Validating table creation
-- MAGIC - Understanding the structure of your data environment
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW TABLES IN landing.power;
-- MAGIC

-- COMMAND ----------

SHOW TABLES IN landing.power

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW VOLUMES — Explore Volumes in a Schema
-- MAGIC
-- MAGIC This SQL command lists all available volumes within a specified schema. Volumes are used in Unity Catalog to manage unstructured or semi-structured data (e.g., files, images, documents) under governance.
-- MAGIC
-- MAGIC In this example, we're listing all volumes in the `power` schema of the `landing` catalog.
-- MAGIC
-- MAGIC Use cases include:
-- MAGIC - Browsing file-based datasets registered under Unity Catalog
-- MAGIC - Validating storage structures available to your workspace
-- MAGIC - Understanding file organization under governed namespaces
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW VOLUMES IN landing.power;
-- MAGIC

-- COMMAND ----------

SHOW VOLUMES IN landing.power

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SHOW GRANT ON VOLUME — View Permissions on a Volume
-- MAGIC
-- MAGIC This SQL command returns the list of access privileges granted on a specific volume. Volumes in Unity Catalog are secure storage locations for file-based data (like CSV, JSON, Parquet), and this command helps you audit who has access and what actions they can perform.
-- MAGIC
-- MAGIC In this case, we are checking the permissions on the `turbine_raw_landing` volume located in the `power` schema of the `landing` catalog.
-- MAGIC
-- MAGIC Use cases include:
-- MAGIC - Auditing data access policies
-- MAGIC - Validating RBAC (Role-Based Access Control) enforcement
-- MAGIC - Ensuring data governance compliance for unstructured data
-- MAGIC
-- MAGIC ```sql
-- MAGIC SHOW GRANT ON VOLUME landing.power.turbine_raw_landing;
-- MAGIC

-- COMMAND ----------

SHOW GRANT ON VOLUME landing.power.turbine_raw_landing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### GRANT Permissions — Allow Access to Tables, Catalogs, and Schemas
-- MAGIC
-- MAGIC These SQL statements assign access rights to users or groups within Unity Catalog. Specifically, this block grants the `account users` group the ability to **query a specific table**, and ensures they have the **necessary usage permissions** on the associated catalog and schema.
-- MAGIC
-- MAGIC This is a critical step in implementing **Role-Based Access Control (RBAC)** in a governed lakehouse environment.
-- MAGIC
-- MAGIC #### Breakdown of Permissions:
-- MAGIC - `GRANT SELECT ON TABLE`: Grants read (query) access to the specified table.
-- MAGIC - `GRANT USAGE ON CATALOG`: Allows visibility and access to the catalog itself — required before accessing any underlying schema or table.
-- MAGIC - `GRANT USAGE ON SCHEMA`: Grants access to the schema where the table resides.
-- MAGIC
-- MAGIC ```sql
-- MAGIC GRANT SELECT ON TABLE ${current_user}_catalog.default.turbine_training_dataset TO `account users`;
-- MAGIC GRANT USAGE ON CATALOG ${current_user}_catalog TO `account users`;
-- MAGIC GRANT USAGE ON SCHEMA ${current_user}_catalog.default TO `account users`;
-- MAGIC

-- COMMAND ----------

-- Let's grant our ANALYSTS a SELECT permission:
-- Note: make sure you created an analysts and dataengineers group first.

GRANT SELECT ON TABLE ${current_user}_catalog.default.turbine_training_dataset TO `account users`;
GRANT USAGE ON CATALOG ${current_user}_catalog TO `account users`;
GRANT USAGE ON SCHEMA ${current_user}_catalog.default TO `account users` ;

-- COMMAND ----------

SHOW GRANT ON ${current_user}_catalog.default.turbine_training_dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unity Catalog Governance: RBAC and Access Control Overview
-- MAGIC
-- MAGIC Unity Catalog in Databricks provides a layered and robust approach to governance and access control that is primarily based on Role-Based Access Control (RBAC), with additional support for Attribute-Based Access Control (ABAC) and fine-grained data protection mechanisms.
-- MAGIC
-- MAGIC ## Key Governance Layers
-- MAGIC
-- MAGIC | Layer                         | Purpose                                                                                   | Mechanisms                                |
-- MAGIC |-------------------------------|-------------------------------------------------------------------------------------------|-------------------------------------------|
-- MAGIC | Workspace-level restrictions  | Limit which workspaces can access specific catalogs and storage credentials                | Workspace-level bindings                  |
-- MAGIC | Privileges and ownership      | Control access to catalogs, schemas, tables, and other objects                             | Privileges assigned to users/groups, object ownership |
-- MAGIC | Attribute-based policies (ABAC) | Dynamically filter/mask data based on policy and governed tags                          | Governed tags, ABAC policies (in Beta)    |
-- MAGIC | Table-level filtering/masking | Restrict rows/columns within tables at query time                                         | Row filters, column masks, dynamic views  |
-- MAGIC
-- MAGIC ## RBAC: Privileges and Object Ownership
-- MAGIC
-- MAGIC - **Privileges:** Access to securable objects (catalogs, schemas, tables, views, functions, etc.) is controlled by assigning privileges such as:
-- MAGIC     - `SELECT`, `MODIFY`, `USE SCHEMA`, and more, granted to individual users or groups.
-- MAGIC - **Ownership:** Each object has an owner with full control, including the ability to:
-- MAGIC     - Read or modify the object and its metadata
-- MAGIC     - Grant or transfer privileges and ownership
-- MAGIC
-- MAGIC - **Delegable and Hierarchical:** 
-- MAGIC     - Owners can delegate privileges without transferring ownership.
-- MAGIC     - Privileges cascade down the hierarchy (e.g., a privilege at the catalog level flows down to schemas and tables).
-- MAGIC
-- MAGIC ## Unity Catalog Admin Roles
-- MAGIC
-- MAGIC - **Account admin:** Manages metastores, identities, account features.
-- MAGIC - **Metastore admin:** Can manage all objects, transfer ownership, assign top-level privileges.
-- MAGIC - **Workspace admin:** Manages workspaces, users, and workspace catalog settings.
-- MAGIC
-- MAGIC ## Attribute-Based Access Control (ABAC) *(Beta)*
-- MAGIC
-- MAGIC - **Policies are based on tags/attributes** such as user role, resource tags, location, etc. This allows fine-grained, dynamic data security.
-- MAGIC - **Centralized, scalable:** Policies can be defined once and enforced across many objects.
-- MAGIC - **Deny rules take precedence** over allow rules for fail-safe security.
-- MAGIC
-- MAGIC ## Fine-Grained Data Protection
-- MAGIC
-- MAGIC - **Row Filters and Column Masks:** Apply filters/masking directly to tables, often using UDFs.
-- MAGIC - **Dynamic Views:** SQL-based logic to restrict rows/columns for additional flexibility, especially when ABAC is not used.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### Practical Tip
-- MAGIC
-- MAGIC A user’s effective permissions are the union of all privileges granted by their individual user account and any groups they belong to. Access is granted only when explicitly allowed—RBAC in Unity Catalog is explicit, not implicit.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC *References: Databricks documentation on access control and governance for Unity Catalog.*
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Unity Catalog Governance: How is it composed in Eni?
-- MAGIC ......
