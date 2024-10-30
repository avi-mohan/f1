# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'f1ClientID')
dbutils.secrets.get(scope = 'formula1-scope', key = 'f1TenantID')
dbutils.secrets.get(scope = 'formula1-scope', key = 'f1clientsecret')

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1ClientID')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1TenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1clientsecret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlll.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1dlll.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1dlll.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1dlll.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1dlll.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlll.dfs.core.windows.net/circuits.csv"))
