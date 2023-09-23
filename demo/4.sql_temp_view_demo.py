# Databricks notebook source
# MAGIC %md
# MAGIC ###Access dataframes using SQL
# MAGIC Objectives
# MAGIC 1. Create temporary views on dataframes 
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell 

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) 
# MAGIC from v_race_results
# MAGIC where race_year=2020

# COMMAND ----------

p_race_year = 2020

# COMMAND ----------

race_results_years_df=spark.sql(f"select * from v_race_results where race_year={p_race_year}")

# COMMAND ----------

display(race_results_years_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global Temporary views
# MAGIC 1. Create global temporary views on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the view from python cell
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from global_temp.gv_race_results;

# COMMAND ----------

spark.sql("select * \
    from global_temp.gv_race_results").show()

# COMMAND ----------
