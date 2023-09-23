# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data lake using cluster scoped credentials
# MAGIC 1.set the spark configure fs.azure.account.key in the cluster
# MAGIC 2.List files from demo container
# MAGIC 3.read data from circuits.csv 
# MAGIC

# COMMAND ----------

 display(dbutils.fs.ls("abfss://demo@formula1deltalakeproject.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalakeproject.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

