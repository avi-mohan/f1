# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1sas-secret')

# COMMAND ----------

f1sas_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1sas-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1dlll.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1dlll.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1dlll.dfs.core.windows.net", f1sas_secret)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net"))

# COMMAND ----------

dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlll.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display
