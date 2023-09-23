# Databricks notebook source
# MAGIC %md
# MAGIC ###Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
.filter("circuit_id < 70")\
.withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")\
.withColumnRenamed("name","race_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "inner")\
.select(circuits_df.circuit_name, circuits_df.location , circuits_df.country , races_df.race_name , races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

display(race_circuits_df.select("circuit_name"))

# COMMAND ----------

#left Outer join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "left")\
.select(circuits_df.circuit_name, circuits_df.location , circuits_df.country , races_df.race_name , races_df.round)


# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#right outer join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "right")\
.select(circuits_df.circuit_name, circuits_df.location , circuits_df.country , races_df.race_name , races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

#full outer join
race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "full")\
.select(circuits_df.circuit_name, circuits_df.location , circuits_df.country , races_df.race_name , races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Semi Joins

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "semi")\
.select(circuits_df.circuit_name, circuits_df.location , circuits_df.country )
#you cant specify right column name here

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anti Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id , "anti")
#which is give you everything on the left dataframe which is not found on the right dataframe

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cross Joins

# COMMAND ----------

race_circuits_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------

int(races_df.count())* int(circuits_df.count())

# COMMAND ----------

