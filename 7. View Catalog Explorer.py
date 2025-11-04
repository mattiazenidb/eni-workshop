# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img style="float:right; margin-left: 10px" src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/bi.png" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/explorer_2.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <img src="https://github.com/mattiazenidb/eni-workshop/raw/main/_resources/lineage.gif" style="float:right; margin-left: 10px" />

# COMMAND ----------

# MAGIC %md
# MAGIC # Unity Catalog Data Lineage Capabilities Overview
# MAGIC
# MAGIC Unity Catalog in Databricks provides powerful data lineage features that enable organizations to track the flow and transformation of data across all workloads and languages, with column-level detail.
# MAGIC
# MAGIC ## Key Features of Data Lineage in Unity Catalog
# MAGIC
# MAGIC - **Runtime Data Lineage:** Captured in near real-time for queries executed via Spark DataFrames, Databricks SQL, notebooks, jobs, and dashboards.
# MAGIC - **Multi-Language Support:** Lineage supports all popular languages used in Databricks including SQL, Python, R, and Scala.
# MAGIC - **Column-Level Lineage:** Tracks data flow down to individual columns where possible, allowing fine-grained traceability.
# MAGIC - **Cross-Workspace Aggregation:** Lineage is aggregated across all workspaces attached to the same Unity Catalog metastore, providing a unified view of data lineage across environments.
# MAGIC - **External Lineage (Public Preview):** Supports including lineage information from external assets and workflows not registered in Unity Catalog.
# MAGIC - **Lineage Visualization:** Interactive lineage graphs can be viewed in Catalog Explorer, showing relationships between tables, columns, and related notebooks or jobs.
# MAGIC - **Programmatic Access:** Lineage metadata can be queried via lineage system tables or accessed through the Databricks REST API.
# MAGIC
# MAGIC ## Requirements and Considerations
# MAGIC
# MAGIC - Tables must be registered in a Unity Catalog metastore to capture lineage.
# MAGIC - Queries must use supported interfaces such as Spark DataFrames or Databricks SQL.
# MAGIC - Users require at least `BROWSE` privilege on the parent catalog of the table or view to see lineage information.
# MAGIC - Compute requirements include Databricks Runtime 11.3 LTS or higher for streaming lineage and 13.3 LTS or higher for certain pipeline workloads.
# MAGIC - Networking settings may need updating for connectivity to Databricks control plane endpoints.
# MAGIC
# MAGIC ## Limitations and Special Cases
# MAGIC
# MAGIC - Some operations such as column lineage in `MERGE` require additional Spark properties enabled and may impact performance.
# MAGIC - Lineage does not capture all SQL constructs (e.g., complex common table expressions, user-defined functions).
# MAGIC - Workspace-level object details (e.g., notebooks, dashboards in other workspaces) may be masked if permission is not granted.
# MAGIC - Lineage data retention is typically one year.
# MAGIC
# MAGIC ## Benefits of Unity Catalog Lineage
# MAGIC
# MAGIC - Enables root cause analysis by tracing data errors back to their sources.
# MAGIC - Supports audit and compliance efforts by providing end-to-end data flow visibility.
# MAGIC - Helps improve data quality and trust by tracking data lineage comprehensively.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Practical Tip
# MAGIC
# MAGIC Use the Catalog Explorer in Databricks to explore interactive lineage graphs visually or query lineage system tables programmatically to build custom lineage reports for governance and auditing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *References: Databricks documentation on data lineage and Unity Catalog governance.*
# MAGIC
