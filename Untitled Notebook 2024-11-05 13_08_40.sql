-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select *, concat(driver_ref, '-', code) as new_driver_ref
from drivers

-- COMMAND ----------

select *, split(name, ' ')
from drivers

-- COMMAND ----------

select nationality, name, dob, RANK() over(partition by nationality order by dob desc) as age_rank
from drivers
order by nationality, age_rank

-- COMMAND ----------


