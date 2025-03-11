# Databricks notebook source
# MAGIC %md
# MAGIC # system.billing.usage
# MAGIC This article provides an overview of the billable usage system table, including the schema and example queries: 
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/billing 

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many DBUs of each SKU were used this month?
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sku_name, usage_date, sum(usage_quantity) as `DBUs`
# MAGIC     FROM system.billing.usage
# MAGIC WHERE
# MAGIC     month(usage_date) = month(NOW())
# MAGIC     AND year(usage_date) = year(NOW())
# MAGIC GROUP BY sku_name, usage_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which jobs consumed the most DBU?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT usage_metadata.job_id as `Job ID`, sum(usage_quantity) as `DBUs`
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_metadata.job_id IS NOT NULL
# MAGIC GROUP BY `Job ID`
# MAGIC ORDER BY `DBUs` DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show me the SKUs growing in usage
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT after.sku_name, before_dbus, after_dbus, ((after_dbus - before_dbus)/before_dbus * 100) AS growth_rate
# MAGIC FROM
# MAGIC (SELECT sku_name, sum(usage_quantity) as before_dbus
# MAGIC     FROM system.billing.usage
# MAGIC WHERE usage_date BETWEEN "2024-11-01" and "2024-11-15"
# MAGIC GROUP BY sku_name) as before
# MAGIC JOIN
# MAGIC (SELECT sku_name, sum(usage_quantity) as after_dbus
# MAGIC     FROM system.billing.usage
# MAGIC WHERE usage_date BETWEEN "2024-12-01" and "2024-12-15"
# MAGIC GROUP BY sku_name) as after
# MAGIC where before.sku_name = after.sku_name
# MAGIC SORT by growth_rate DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## What consumption is assigned to resources with a certain tag?
# MAGIC
# MAGIC tagging best practices: 
# MAGIC https://www.databricks.com/blog/best-practices-cost-management-databricks
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/cost-optimization/best-practices#tag-clusters-for-cost-attribution
# MAGIC
# MAGIC
# MAGIC Databricks System Table Deep Dive — Data Tagging Design Patterns: https://medium.com/dbsql-sme-engineering/databricks-system-table-deep-dive-data-tagging-design-patterns-652718842b82 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sku_name, usage_unit, SUM(usage_quantity) as `DBUs consumed`
# MAGIC FROM system.billing.usage
# MAGIC WHERE custom_tags.{{key}} = "{{value}}"
# MAGIC GROUP BY 1, 2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which cluster owners use the most DBUs?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.record_id record_id,
# MAGIC   c.cluster_id cluster_id,
# MAGIC   max_by(c.owned_by, c.change_time) owned_by,
# MAGIC   max(c.change_time) change_time,
# MAGIC   any_value(u.usage_start_time) usage_start_time,
# MAGIC   any_value(u.usage_quantity) usage_quantity
# MAGIC FROM
# MAGIC   system.billing.usage u
# MAGIC   JOIN system.compute.clusters c
# MAGIC WHERE
# MAGIC   u.usage_metadata.cluster_id is not null
# MAGIC   and u.usage_start_time >= '2023-01-01'
# MAGIC   and u.usage_metadata.cluster_id = c.cluster_id
# MAGIC   and c.change_time <= u.usage_start_time
# MAGIC GROUP BY 1, 2
# MAGIC ORDER BY cluster_id, usage_start_time desc;

# COMMAND ----------

# MAGIC %md
# MAGIC # Monthly budget notification 
# MAGIC As an administrator, I want to receive a notification if the monthly budget is about to be consumed
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select record_id, usage_quantity as dbu_used_ratio
# MAGIC from system.billing.usage

# COMMAND ----------

# MAGIC %md
# MAGIC imagibe you have 1000000 DBU as your monthly budget

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(usage_quantity)/ 1000000 as dbu_used_ratio
# MAGIC from system.billing.usage

# COMMAND ----------

# MAGIC %md
# MAGIC above cell looks at all DBU cosumption of all time, let's look at only last month

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(usage_quantity) / 1000000 as dbu_used_ratio
# MAGIC from system.billing.usage
# MAGIC where usage_date > date_trunc("month", now())

# COMMAND ----------

# MAGIC %md
# MAGIC let's look at only specific workspace you want to monitor.
# MAGIC
# MAGIC replace {{workspace_id}} with your target workspace 

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(usage_quantity) / 1000000 as dbu_used_ratio
# MAGIC from system.billing.usage
# MAGIC where usage_date > date_trunc("month", now())
# MAGIC   and workspace_id = '{{workspace_id}}'

# COMMAND ----------

# MAGIC %md
# MAGIC if you want to create an alert, 
# MAGIC - Save the query as " budget alert"
# MAGIC - New 
# MAGIC - Alert 
# MAGIC
# MAGIC ![Screenshot 2024-12-17 at 10.55.53 AM.png](./Screenshot 2024-12-17 at 10.55.53 AM.png "Screenshot 2024-12-17 at 10.55.53 AM.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL Warehouse Advisor Dashboard
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC In this article, you learn how to use the warehouses system table to monitor and manage the SQL warehouses in your workspaces. You can look at warehouse statistics such as: 
# MAGIC - Query time (average)
# MAGIC - Query errors 
# MAGIC - Slowest queries 
# MAGIC
# MAGIC
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/warehouses 
# MAGIC
# MAGIC
# MAGIC Inspired by this sytem table there is a dashboard template to help you monitor warehouse activities
# MAGIC “SQL Warehouse Advisor Dashboard” 
# MAGIC
# MAGIC - https://medium.com/dbsql-sme-engineering/the-new-databricks-sql-warehouse-advisor-dashboard-a89771bbee3e 
# MAGIC - https://medium.com/dbsql-sme-engineering/dbsql-warehouse-advisor-updates-v4-now-with-dashboard-tabs-18a65e69b47b
# MAGIC
# MAGIC dashboard template:
# MAGIC - https://github.com/CodyAustinDavis/dbsql_sme/tree/main/Observability%20Dashboards%20and%20DBA%20Resources/Observability%20Lakeview%20Dashboard%20Templates/DBSQL%20Warehouse%20Advisor%20With%20Data%20Model 
# MAGIC
# MAGIC ![Screenshot 2024-12-17 at 2.00.05 PM.png](./Screenshot 2024-12-17 at 2.00.05 PM.png "Screenshot 2024-12-17 at 2.00.05 PM.png")

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
