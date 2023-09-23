# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data lake using sas token
# MAGIC 1.set the spark configure fs.azure.account.key
# MAGIC 2.List files from demo container
# MAGIC 3.read data from circuits.csv 
# MAGIC

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope='formula1-scopes',key='formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1deltalakeproject.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1deltalakeproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1deltalakeproject.dfs.core.windows.net", formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1deltalakeproject.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalakeproject.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

