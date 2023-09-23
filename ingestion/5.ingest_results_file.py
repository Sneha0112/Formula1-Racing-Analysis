# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest Results.json file

# COMMAND ----------

spark.read.json("/mnt/formula1deltalakeproject/raw/2021-03-21/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_cutover
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

spark.read.json("/mnt/formula1deltalakeproject/raw/2021-03-28/results.json").createOrReplaceTempView("results_w1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_w1
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

spark.read.json("/mnt/formula1deltalakeproject/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId,count(1)
# MAGIC from results_w2
# MAGIC group by raceId
# MAGIC order by raceId desc;

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#spark.read.json("/mnt/formula1deltalakeproject/raw/2021-04-18/results.json").createOrReplaceTempView("results_w2")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Read the json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType , StructField , IntegerType , StringType , FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", StringType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("data_source",StringType(),True),
    StructField("statusId",StringType(),True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

results_with_columns_df=results_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId", "driver_id") .withColumnRenamed("constructorId", "constructor_id") .withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed").withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Drop the unwanted Column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df    = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe DataFrame

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

#output_df=re_arrange_partition_column(results_final_df,'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 1

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
#  if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id ={race_id_list.race_id})")

# COMMAND ----------

#results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Method 2

# COMMAND ----------

#overwrite_partition(results_final_df,'f1_processed','results','race_id')

# COMMAND ----------

merge_condition= "tgt.result_id=src.result_id AND tgt.race_id =src.race_id"
merge_delta_data(results_deduped_df,'f1_processed','results',processed_folder_path,merge_condition,'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")


# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)
# MAGIC from f1_processed.results
# MAGIC where file_date='2021-03-21';

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id , driver_id ,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id,driver_id
# MAGIC having count(1) > 1
# MAGIC order by race_id,driver_id desc;

# COMMAND ----------

# MAGIC  %sql
# MAGIC --  drop table f1_processed.results;

# COMMAND ----------
