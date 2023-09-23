# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuit_file",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_races_file",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest_constructors_files",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_drivers_file",0,{"p_data_source": "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file",0,{"p_data_source":  "Ergast API","p_file_date":"2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1) from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

