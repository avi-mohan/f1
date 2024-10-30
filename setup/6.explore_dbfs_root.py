# Databricks notebook source
# MAGIC %md
# MAGIC ###Explore DBFS root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. Interact with DBFS File Browser
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

display(spark.read.csv('/FileStore/tables/circuits.csv'))

# COMMAND ----------


