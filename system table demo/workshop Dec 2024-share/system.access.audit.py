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
# MAGIC WHERE user_identity.email = 'razi.bayati@databricks.com'
# MAGIC         AND action_name IN ('createTable', 'commandSubmit','getTable','deleteTable')
# MAGIC         -- AND datediff(now(), event_date) < 1
# MAGIC         -- ORDER BY event_date DESC
# MAGIC

# COMMAND ----------



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
# MAGIC WHERE (request_params.full_name_arg = 'users.razi_bayati.bronze_facilities_table'
# MAGIC   OR (request_params.name = 'bronze_facilities_table'
# MAGIC   AND request_params.schema_name = 'razi_bayati'))
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

# MAGIC %sql
# MAGIC SELECT DISTINCT service_name 
# MAGIC FROM system.access.audit

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
# MAGIC # system.information_schema.

# COMMAND ----------

# MAGIC
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
# MAGIC # dbsql events
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
# MAGIC # dashabord events
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
