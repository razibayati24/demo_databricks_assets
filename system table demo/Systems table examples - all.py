# Databricks notebook source
# MAGIC %md
# MAGIC # system.access.audit
# MAGIC
# MAGIC This article outlines the audit log table schema and has sample queries you can use with the audit log system table to answer common account usage questions: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which tables did a user access?
# MAGIC
# MAGIC replace {{user}} with user's email address and thsi query with return all table a user have accessed; you can also add/remove action_name to look into particualr action. 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC         action_name as `EVENT`,
# MAGIC         event_time as `WHEN`,
# MAGIC         IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
# MAGIC         IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
# MAGIC FROM system.access.audit
# MAGIC WHERE user_identity.email = '{{User}}'
# MAGIC         AND action_name IN ('createTable', 'commandSubmit','getTable','deleteTable')
# MAGIC         -- AND datediff(now(), event_date) < 1
# MAGIC         -- ORDER BY event_date DESC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which users accessed a table within the last day?
# MAGIC
# MAGIC replace {{catalog.schema.table}} , {{table_name}} and {{schema_name}} with the target table information

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   user_identity.email as `User`,
# MAGIC   IFNULL(request_params.full_name_arg,
# MAGIC     request_params.name)
# MAGIC     AS `Table`,
# MAGIC     action_name AS `Type of Access`,
# MAGIC     event_time AS `Time of Access`
# MAGIC FROM system.access.audit
# MAGIC WHERE (request_params.full_name_arg = '{{catalog.schema.table}}'
# MAGIC   OR (request_params.name = '{{table_name}}'
# MAGIC   AND request_params.schema_name = '{{schema_name}}'))
# MAGIC   AND action_name
# MAGIC     IN ('createTable','getTable','deleteTable')
# MAGIC   AND event_date > now() - interval '1 day'
# MAGIC ORDER BY event_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## View permissions changes for all securable objects 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, mask(user_identity.email), request_params.securable_type, request_params.securable_full_name, request_params.changes
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND action_name = 'updatePermissions'
# MAGIC ORDER BY 1 DESC
# MAGIC --LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC let's look at all disticnt service_names in system.access.audit
# MAGIC
# MAGIC - Workspace level: https://docs.databricks.com/en/admin/account-settings/audit-logs.html#workspace-level-services 
# MAGIC - Account level: https://docs.databricks.com/en/admin/account-settings/audit-logs.html#workspace-level-services

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC let's look at all distinct action_names in system.access.audit

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## View the most recently run notebook commands
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, mask(user_identity.email), request_params.commandText
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'runCommand'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which users have logged into a Databricks?
# MAGIC
# MAGIC you can add another filter to only look at specific app, replace {{application-ID}} with the Application ID value for the service principal assigned to a specific Databricks app. This value can be found in the admin settings for the Databricks workspace hosting the app.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   workspace_id,
# MAGIC   request_params.request_object_id as app,
# MAGIC   user_identity.email as user_email,
# MAGIC   user_identity.subject_name as username
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   action_name IN ("workspaceInHouseOAuthClientAuthentication", "mintOAuthToken", "mintOAuthAuthorizationCode")
# MAGIC --AND
# MAGIC   --request_params["client_id"] LIKE "{{application-ID}}"
# MAGIC GROUP BY
# MAGIC   event_date,
# MAGIC   workspace_id,
# MAGIC   app,
# MAGIC   user_email,
# MAGIC   username

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which Databricks apps have been updated to change how the app is shared with other users or groups?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   event_date,
# MAGIC   workspace_id,
# MAGIC   request_params['request_object_id'] as app,
# MAGIC   user_identity['email'] as sharing_user,
# MAGIC   acl_entry['group_name'],
# MAGIC   acl_entry['user_name'],
# MAGIC   acl_entry['permission_level']
# MAGIC FROM
# MAGIC   system.access.audit t
# MAGIC LATERAL VIEW
# MAGIC   explode(from_json(request_params['access_control_list'], 'array<struct<user_name:string,permission_level:string,group_name:string>>')) acl_entry AS acl_entry
# MAGIC WHERE
# MAGIC   action_name = 'changeAppsAcl'
# MAGIC AND
# MAGIC   request_params['request_object_type'] = 'apps'
# MAGIC ORDER BY
# MAGIC   event_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC # system.information_schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Who can access this table? 
# MAGIC replace {{schema_name}} and {{table_name}} with the table information you want to query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(grantee) AS `ACCESSIBLE BY`
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_schema = '{{schema_name}}' AND table_name = '{{table_name}}'
# MAGIC   UNION
# MAGIC     SELECT table_owner
# MAGIC     FROM system.information_schema.tables
# MAGIC     WHERE table_schema = '{{schema_name}}' AND table_name = '{{table}}'
# MAGIC   UNION
# MAGIC     SELECT DISTINCT(grantee)
# MAGIC     FROM system.information_schema.schema_privileges
# MAGIC     WHERE schema_name = '{{schema_name}}'

# COMMAND ----------

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
# MAGIC # system.query.history
# MAGIC
# MAGIC The query history table includes records for every SQL statement run using SQL warehouses.  
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/query-history 

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

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT(workspace_id)
# MAGIC FROM
# MAGIC   system.access.audit

# COMMAND ----------

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
# MAGIC  AND source_column_name = {{column_val}}
# MAGIC  AND source_table_full_name = {{table_full_name_val}}
# MAGIC  AND datediff(now(), event_date) < 90
# MAGIC GROUP BY
# MAGIC  ALL

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## New users in workspace
# MAGIC New users in workspace: You can use system.access.audit table to get details about users per workspace. You may need to add filters to get the info you want. Below is sample query for active users:
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   workspace_id,
# MAGIC   COUNT(DISTINCT user_identity.email) as user_count
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   service_name = 'accounts'
# MAGIC   and action_name = 'tokenLogin'
# MAGIC   and request_params.user like '%@%'
# MAGIC GROUP BY workspace_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbsql events
# MAGIC
# MAGIC reference: https://docs.databricks.com/en/admin/account-settings/audit-logs.html#databricks-sql-events

# COMMAND ----------

# MAGIC %md
# MAGIC ### long running queries in notebook
# MAGIC
# MAGIC Identify long-running queries and track inefficient code running through Notebooks with System Tables and Lakeview.
# MAGIC
# MAGIC The events we need are tracked with the commandSubmit, commandFinish, runCommand events 
# MAGIC
# MAGIC - Step 1 : Activate verbose audit logs
# MAGIC ![Screenshot 2024-12-17 at 11.10.10 AM.png](./Screenshot 2024-12-17 at 11.10.10 AM.png "Screenshot 2024-12-17 at 11.10.10 AM.png")
# MAGIC
# MAGIC Once enabled, Databricks will populate the system.access.audit table with the events in question. We can then run the queries and create visualizations to spot areas where we need to pay more attention to reduce costs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    event_date,
# MAGIC    user_identity.email,
# MAGIC    request_params.notebookId, request_params.clusterId, request_params.executionTime,
# MAGIC    request_params.status, request_params.commandLanguage, request_params.commandId,
# MAGIC    request_params.commandText
# MAGIC FROM system.access.audit
# MAGIC WHERE 1=1
# MAGIC    AND action_name = 'runCommand'
# MAGIC    AND request_params.status NOT IN ('skipped')
# MAGIC    AND TIMESTAMPDIFF(HOUR, event_date, CURRENT_TIMESTAMP()) < 24 * 90
# MAGIC ORDER BY request_params.executionTime DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC    action_name as `EVENT`,
# MAGIC    event_time as `WHEN`,
# MAGIC    IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
# MAGIC    IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
# MAGIC FROM system.access.audit
# MAGIC WHERE user_identity.email like 'razi.bayati@databricks.com'
# MAGIC    AND action_name IN ('createTable', 'commandSubmit','getTable','deleteTable')
# MAGIC    AND datediff(now(), event_date) < 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## dashabord events
# MAGIC
# MAGIC reference: https://docs.databricks.com/en/admin/account-settings/audit-logs.html#dashboards-events
# MAGIC
# MAGIC example: https://learn.microsoft.com/en-us/azure/databricks/dashboards/admin/audit-logs

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many dashboards were created in the past week?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   action_name,
# MAGIC   COUNT(action_name) as num_dashboards
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   action_name = "createDashboard"
# MAGIC   AND event_date >= current_date() - interval 7 days
# MAGIC GROUP BY
# MAGIC   action_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are the dashboard ids associated with the most popular dashboards?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   request_params.dashboard_id as dashboard_id,
# MAGIC   COUNT(*) AS view_count
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   action_name in ("getDashboard", "getPublishedDashboard")
# MAGIC GROUP BY
# MAGIC   dashboard_id
# MAGIC ORDER BY
# MAGIC   view_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many times was this dashboard viewed in the past week?
# MAGIC
# MAGIC uses a specific {{dashboard_id}} to show the number of times the dashboard was viewed in the past week. Substitute <dashboard_id> with the UUID string associated with a dashboard in your workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   action_name,
# MAGIC   COUNT(action_name) as view_count
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   request_params.dashboard_id = "<dashboard_id>"
# MAGIC   AND event_date >= current_date() - interval 7 days
# MAGIC   AND action_name in ("getDashboard", "getPublishedDashboard")
# MAGIC GROUP BY action_name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Who were the top viewers in the past week?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   user_identity.email as user_email,
# MAGIC   action_name,
# MAGIC   COUNT(action_name) as view_count
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   request_params.dashboard_id = "<dashboard_id>"
# MAGIC   AND event_date >= current_date() - interval 7 days
# MAGIC   AND action_name in ("getDashboard", "getPublishedDashboard")
# MAGIC GROUP BY action_name, user_email

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor embedded dashboards
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/dashboards/embed 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   request_params.settingTypeName,
# MAGIC   source_ip_address,
# MAGIC   user_identity.email,
# MAGIC   action_name,
# MAGIC   request_params
# MAGIC FROM
# MAGIC   system.access.audit
# MAGIC WHERE
# MAGIC   request_params.settingTypeName ilike "aibi%"

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
