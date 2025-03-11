# Databricks notebook source
# MAGIC %md
# MAGIC # system.compute.warehouses

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which warehouses were created this week?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     warehouse_id,
# MAGIC     warehouse_name,
# MAGIC     warehouse_type,
# MAGIC     warehouse_channel,
# MAGIC     warehouse_size,
# MAGIC     min_clusters,
# MAGIC     max_clusters,
# MAGIC     auto_stop_minutes,
# MAGIC     tags,
# MAGIC     change_time as datetime_created,
# MAGIC     delete_time
# MAGIC FROM
# MAGIC     system.compute.warehouses
# MAGIC QUALIFY
# MAGIC     ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY change_time ASC) = 1
# MAGIC     and change_time >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL 7 days
# MAGIC     and delete_time is null;

# COMMAND ----------

# MAGIC %md
# MAGIC This article provides you with a reference guide for the compute system tables. You can use these tables to monitor the activity and metrics of all-purpose and jobs compute in your account:
# MAGIC
# MAGIC - clusters: Records compute configurations in your account.
# MAGIC - node_types: Includes a single record for each of the currently available node types, including hardware information.
# MAGIC - node_timeline: Includes minute-by-minute records of your compute’s utilization metrics.
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/compute

# COMMAND ----------

# MAGIC %md
# MAGIC # system.compute.clusters and system.billing.usage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join cluster records with the most recent billing records
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.record_id,
# MAGIC   c.cluster_id,
# MAGIC   c.owned_by,
# MAGIC   c.change_time,
# MAGIC   u.usage_start_time,
# MAGIC   u.usage_quantity
# MAGIC FROM
# MAGIC   system.billing.usage u
# MAGIC   JOIN system.compute.clusters c
# MAGIC   JOIN (SELECT u.record_id, c.cluster_id, max(c.change_time) change_time
# MAGIC     FROM system.billing.usage u
# MAGIC     JOIN system.compute.clusters c
# MAGIC     WHERE
# MAGIC       u.usage_metadata.cluster_id is not null
# MAGIC       and u.usage_start_time >= '2023-01-01'
# MAGIC       and u.usage_metadata.cluster_id = c.cluster_id
# MAGIC       and date_trunc('HOUR', c.change_time) <= date_trunc('HOUR', u.usage_start_time)
# MAGIC     GROUP BY all) config
# MAGIC WHERE
# MAGIC   u.usage_metadata.cluster_id is not null
# MAGIC   and u.usage_start_time >= '2023-01-01'
# MAGIC   and u.usage_metadata.cluster_id = c.cluster_id
# MAGIC   and u.record_id = config.record_id
# MAGIC   and c.cluster_id = config.cluster_id
# MAGIC   and c.change_time = config.change_time
# MAGIC ORDER BY cluster_id, usage_start_time desc;
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
