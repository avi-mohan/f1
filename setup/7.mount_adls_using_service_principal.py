# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id, and client_secret from key vault
# MAGIC 2. Set Spark config with App/Client id, Directory/Tenant Id & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all file mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1ClientID')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1TenantID')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'f1clientsecret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1dlll.dfs.core.windows.net/",
  mount_point = "/mnt/f1dlll/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1dlll/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/f1dlll/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/f1dlll/demo')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


