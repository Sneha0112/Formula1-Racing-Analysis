# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data lake using access keys
# MAGIC 1.set the spark configure fs.azure.account.key
# MAGIC 2.List files from demo container
# MAGIC 3.read data from circuits.csv 
# MAGIC

# COMMAND ----------

formula1dl_account_key = dbutils.secrets.get(scope='formula1-scopes',key='formula1vaultkey')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1deltalakeproject.dfs.core.windows.net",
    formula1dl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1deltalakeproject.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalakeproject.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

