# Databricks notebook source
# MAGIC %md
# MAGIC # system.access.table_lineage and .column_lineage
# MAGIC This article provides an overview of the two lineage system tables. These system tables build on Unity Catalog’s data lineage feature, allowing you to programmatically query lineage data to fuel decision making and reports.
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/lineage

# COMMAND ----------

# MAGIC %md
# MAGIC Understanding Data Access Patterns with Unity Catalog Lineage using sql queries: 
# MAGIC
# MAGIC reference: https://medium.com/dbsql-sme-engineering/understanding-data-access-patterns-with-unity-catalog-lineage-52eb4a632700

# COMMAND ----------

# MAGIC %md
# MAGIC ### Object Popularity
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC substitude **source_table_catalog** with catalouge you are interested in, here we look into **system** catalog: The most popular system table objects, measured by a simple count of accesses over the last 7 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  source_table_full_name,
# MAGIC  count(*) as lineage_total
# MAGIC FROM
# MAGIC  system.access.table_lineage
# MAGIC WHERE
# MAGIC  datediff(now(), event_date) < 7
# MAGIC  AND source_table_catalog = 'system'
# MAGIC GROUP BY
# MAGIC  ALL
# MAGIC ORDER by
# MAGIC  lineage_total DESC
# MAGIC LIMIT
# MAGIC  5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Accesses of tables in a specific catalog and schema:   
# MAGIC
# MAGIC
# MAGIC Show me counts of accesses for all tables within a particular catalog and schema, you can substitude **source_table_catalog** and **source_table_schema** values with you taregt schema and cataloge 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   source_table_name,
# MAGIC   entity_type,
# MAGIC   created_by,
# MAGIC   source_type,
# MAGIC   COUNT(distinct event_time) as access_count,
# MAGIC   MIN(event_date) as first_access_date,
# MAGIC   MAX(event_date) as last_access_date
# MAGIC FROM
# MAGIC   system.access.table_lineage
# MAGIC WHERE
# MAGIC   source_table_catalog = "system"
# MAGIC   AND source_table_schema = "access"
# MAGIC   AND datediff(now(), event_date) < 30
# MAGIC GROUP BY
# MAGIC   ALL
# MAGIC ORDER BY
# MAGIC   ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column Level Analyses
# MAGIC
# MAGIC Column references:
# MAGIC The most referenced columns and their tables in a catalog over the last 90 days
# MAGIC
# MAGIC substitude **source_table_catalog** with catalouge you are interested in, here we look into **system** catalog:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  source_column_name,
# MAGIC  source_table_full_name,
# MAGIC  COUNT(*) AS frequency
# MAGIC FROM
# MAGIC  system.access.column_lineage
# MAGIC WHERE
# MAGIC  1 = 1
# MAGIC  AND source_type <> 'PATH'
# MAGIC  AND datediff(now(), event_date) < 90
# MAGIC  AND source_table_catalog = 'system'
# MAGIC GROUP BY
# MAGIC  source_column_name,
# MAGIC  source_table_full_name
# MAGIC ORDER BY
# MAGIC  frequency DESC
# MAGIC LIMIT
# MAGIC  10

# COMMAND ----------

# MAGIC %md
# MAGIC You can use the Insights tab in Catalog Explorer to view the most frequent recent queries and users of any table registered in Unity Catalog.  
# MAGIC
# MAGIC https://docs.databricks.com/en/discover/table-insights.html 
# MAGIC
# MAGIC ![Screenshot 2024-12-17 at 1.16.33 PM.png](./Screenshot 2024-12-17 at 1.16.33 PM.png "Screenshot 2024-12-17 at 1.16.33 PM.png")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![Screenshot 2024-12-17 at 1.16.42 PM.png](./Screenshot 2024-12-17 at 1.16.42 PM.png "Screenshot 2024-12-17 at 1.16.42 PM.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Object Lineage 
# MAGIC
# MAGIC This article describes how to capture and visualize data lineage using Catalog Explorer, the data lineage system tables, and the REST API: 
# MAGIC https://docs.databricks.com/en/data-governance/unity-catalog/data-lineage.html#capture-and-view-data-lineage-using-unity-catalog 
# MAGIC
# MAGIC Lineage is available in the Catalog UI, where the interface expresses upstream and downstream relationships as visual graphs.  
# MAGIC
# MAGIC ![Screenshot 2024-12-17 at 1.21.30 PM.png](./Screenshot 2024-12-17 at 1.21.30 PM.png "Screenshot 2024-12-17 at 1.21.30 PM.png")
# MAGIC
# MAGIC you can also query the lineage using system tables, you can query table lineage to see upstream/downstream tables and analyze data usage, such as the who, when, and how users access data. The example below will show all accesses to the billing usage table over the last seven days:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  *
# MAGIC FROM
# MAGIC  system.access.table_lineage
# MAGIC WHERE
# MAGIC  source_table_full_name = 'system.billing.usage'
# MAGIC AND datediff(now(), event_date) < 7

# COMMAND ----------

# MAGIC %md
# MAGIC ### What objects depend on a specific column?
# MAGIC
# MAGIC The column reads and target downstream columns/tables from a single column over the last 90 days.
# MAGIC Substitude {{column_val}}, {{table_full_name_val}} and {{target_catalog_val}} with your desired values
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  target_column_name,
# MAGIC  target_table_full_name,
# MAGIC  COUNT(*) AS frequency
# MAGIC FROM
# MAGIC  system.access.column_lineage
# MAGIC WHERE
# MAGIC  1 = 1
# MAGIC  AND source_column_name = {{column_val}}
# MAGIC  AND source_table_full_name = {{table_full_name_val}} 
# MAGIC  AND datediff(now(), event_date) < 90
# MAGIC  AND target_table_full_name IS NOT NULL
# MAGIC  AND target_table_catalog = {{target_catalog_val}}
# MAGIC  GROUP BY
# MAGIC  target_column_name,
# MAGIC  target_table_full_name
# MAGIC ORDER BY
# MAGIC  frequency DESC
# MAGIC LIMIT
# MAGIC  10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC I want to see if **source_column_name** = 'facility_name'
# MAGIC in **source_table_full_name** = 'razi_demo.vch.facilities_table' has been used in **target_table_catalog** = 'razi_demo'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  target_column_name,
# MAGIC  target_table_full_name,
# MAGIC  COUNT(*) AS frequency
# MAGIC FROM
# MAGIC  system.access.column_lineage
# MAGIC WHERE
# MAGIC  1 = 1
# MAGIC  AND source_column_name = 'facility_name'
# MAGIC  AND source_table_full_name = 'razi_demo.vch.facilities_table'
# MAGIC  AND datediff(now(), event_date) < 90
# MAGIC  AND target_table_full_name IS NOT NULL
# MAGIC  AND target_table_catalog = 'razi_demo'
# MAGIC  GROUP BY
# MAGIC  target_column_name,
# MAGIC  target_table_full_name
# MAGIC ORDER BY
# MAGIC  frequency DESC
# MAGIC LIMIT
# MAGIC  10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column access — frequency + users:
# MAGIC Accesses of a single column/table over the last 90 days — in this case, accesses of the usage column within the billing tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC  event_date,
# MAGIC  COUNT(*) AS frequency
# MAGIC FROM
# MAGIC  system.access.column_lineage
# MAGIC WHERE
# MAGIC  1 = 1
# MAGIC  AND source_column_name = 'facility_name'
# MAGIC  AND source_table_full_name = 'razi_demo.vch.facilities_table'
# MAGIC  AND datediff(now(), event_date) < 90
# MAGIC GROUP BY
# MAGIC  ALL

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC # Introduction to Databricks Lakehouse Monitoring
# MAGIC
# MAGIC %md
# MAGIC This article describes Databricks Lakehouse Monitoring. It covers the benefits of monitoring your data and gives an overview of the components and usage of Databricks Lakehouse Monitoring. 
# MAGIC
# MAGIC https://docs.databricks.com/en/lakehouse-monitoring/index.html#introduction-to-databricks-lakehouse-monitoring 
# MAGIC
# MAGIC
# MAGIC ![Screenshot 2024-12-17 at 2.00.42 PM.png](./Screenshot 2024-12-17 at 2.00.42 PM.png "Screenshot 2024-12-17 at 2.00.42 PM.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # other resources 
# MAGIC - Top 10 Queries to use with System Tables: https://community.databricks.com/t5/technical-blog/top-10-queries-to-use-with-system-tables/ba-p/82331   
# MAGIC - Dashbaord templates: https://github.com/CodyAustinDavis/dbsql_sme/tree/main 
# MAGIC
# MAGIC - Databricks System Table Deep Dive — Data Tagging Design Patterns: https://medium.com/dbsql-sme-engineering/databricks-system-table-deep-dive-data-tagging-design-patterns-652718842b82 
# MAGIC
# MAGIC - Understanding Data Access Patterns with Unity Catalog Lineage: https://medium.com/dbsql-sme-engineering/understanding-data-access-patterns-with-unity-catalog-lineage-52eb4a632700
# MAGIC
# MAGIC Demos:
# MAGIC - Monitor Your Data Quality With Lakehouse Monitoring: https://www.databricks.com/resources/demos/tutorials/data-warehouse-and-bi/monitor-your-data-quality-with-lakehouse-monitoring?itm_data=demo_center 
# MAGIC
# MAGIC - System Tables: Billing Forecast, Usage Analytics, and Access Auditing With Databricks Unity Catalog: https://www.databricks.com/resources/demos/tutorials/governance/system-tables?itm_data=demo_center
# MAGIC
