# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Warehouse Advisor Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC The query history table includes records for every SQL statement run using SQL warehouses.  
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/query-history 
# MAGIC
# MAGIC You can look at warehouse statistics such as: 
# MAGIC - Query time (average)
# MAGIC - Query errors 
# MAGIC - Slowest queries 
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


