# Databricks notebook source
# MAGIC %md
# MAGIC ####1. Write data to delta lake (managed table)
# MAGIC ####2. Write data to delta lake (external table)
# MAGIC ####3. Read data from delta lake (Table)
# MAGIC ####4. Read data from delta lake (File)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demod
# MAGIC location '/mnt/formula1deltalakeproject/demod'  

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1deltalakeproject/raw/2021-03-28/results.json")


# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demod.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1deltalakeproject/demod/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demod.results_external 
# MAGIC using delta
# MAGIC location '/mnt/formula1deltalakeproject/demod/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_external;

# COMMAND ----------

results_external_df=spark.read.format("delta").load("/mnt/formula1deltalakeproject/demod/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demod.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demod.results_partitioned;

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.Update delta table
# MAGIC #####2.Delete from delta table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demod.results_managed
# MAGIC set points=11-position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

#update in pyhton
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1deltalakeproject/demod/results_managed")
deltaTable.update("position <= 10",{"points":"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demod.results_managed
# MAGIC where position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

#pyhton delete

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1deltalakeproject/demod/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #####Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema",True)\
.json("/mnt/formula1deltalakeproject/raw/2021-03-28/drivers.json")\
.filter("driverId <= 10")\
.select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")


# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema",True)\
.json("/mnt/formula1deltalakeproject/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 6 AND 15")\
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1deltalakeproject/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demod.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demod.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC             tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC  WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,"/mnt/formula1deltalakeproject/demod/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. History and Versioning
# MAGIC ###2. Time Travel
# MAGIC ###3.  Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demod.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge timestamp as of '2023-08-11T13:57:45.000+0000';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf",'2023-08-11T13:57:45.000+0000').load("/mnt/formula1deltalakeproject/demod/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demod.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC vacuum f1_demod.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC  %sql
# MAGIC  delete from f1_demod.drivers_merge where driverId=1;      

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge ;  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demod.drivers_merge tgt
# MAGIC using f1_demod.drivers_merge version as of 3 src
# MAGIC on (tgt.driverID=src.driverId)
# MAGIC when not matched then
# MAGIC   insert * 

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demod.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demod.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transaction log

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demod.drivers_txn(
# MAGIC   driverId int,
# MAGIC   dob date,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createdDate date,
# MAGIC   updatedDate date)
# MAGIC   using delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demod.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demod.drivers_txn
# MAGIC select * from f1_demod.drivers_merge
# MAGIC where driverId=1  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demod.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demod.drivers_txn
# MAGIC select * from f1_demod.drivers_merge
# MAGIC where driverId=2  ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demod.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demod.drivers_txn
# MAGIC where driverId=1  

# COMMAND ----------

# MAGIC %md
# MAGIC ####Convert parquet to delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demod.drivers_convert_to_delta(
# MAGIC driverId int,
# MAGIC dob date,
# MAGIC forename string,
# MAGIC surname string,
# MAGIC createDate date,
# MAGIC updateDate date
# MAGIC )
# MAGIC using parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demod.drivers_convert_to_delta
# MAGIC select * from f1_demod.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta f1_demod.drivers_convert_to_delta

# COMMAND ----------

#created parquet file
df=spark.table("f1_demod.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("/mnt/formula1deltalakeproject/demod/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/mnt/formula1deltalakeproject/demod/drivers_convert_to_delta_new`

# COMMAND ----------

