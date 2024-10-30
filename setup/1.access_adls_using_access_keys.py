# Databricks notebook source
# MAGIC %md 
# MAGIC #### access azure data lake using access keys
# MAGIC 1. set spark config fs.azure.account.key
# MAGIC 2. list files from demo container
# MAGIC 3. read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlaccountkey')

# COMMAND ----------

formula1dlaccountkey = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dlaccountkey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1dlll.dfs.core.windows.net", formula1dlaccountkey)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1dlll.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1dlll.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display
