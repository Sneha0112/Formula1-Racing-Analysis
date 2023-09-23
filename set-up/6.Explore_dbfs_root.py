# Databricks notebook source
# MAGIC %md
# MAGIC ####EXplore DBFS Root
# MAGIC 1.List all the folders in dbfs root
# MAGIC 2.Interact with DBFS File browser
# MAGIC 3.Upload File to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------

