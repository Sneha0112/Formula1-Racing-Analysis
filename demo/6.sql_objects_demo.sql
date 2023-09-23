-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Lesson Objectives
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command 
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. find the current database

-- COMMAND ----------

create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

use demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Learning Objective
-- MAGIC 1. create managed table using python
-- MAGIC 2. create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")    

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;
show tables;

-- COMMAND ----------

describe extended race_results_python;

-- COMMAND ----------

   select * from demo.race_results_python where race_year=2020;

-- COMMAND ----------

create table demo.race_results_sql
as
select * from demo.race_results_python
where race_year=2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Learning Objective
-- MAGIC 1. Create external table using python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping in external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(
  race_year INT,
  race_name String,
  race_date timestamp,
  circuit_location string,
  driver_name string,
  driver_number int,
  driver_nationality string,
  team string,
  grid int,
  fastest_lap int,
  race_time string,
  points float,
  position int,
  created_date timestamp
)
using parquet
location "/mnt/formula1deltalakeproject/presentation/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

insert into demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year=2020;

-- COMMAND ----------

select count(1) from demo.race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

drop table demo.race_results_ext_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Views on tables
-- MAGIC 1. Create temp view
-- MAGIC 2. Create global temp view
-- MAGIC 3. Create Permanent view 

-- COMMAND ----------

select current_database();

-- COMMAND ----------

create or replace temp view  v_race_results
as 
select *
 from demo.race_results_python
 where race_year=2010;

-- COMMAND ----------

select * from v_race_results;

-- COMMAND ----------

create or replace global temp view  gv_race_results
as 
select *
 from demo.race_results_python
 where race_year=2018;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

create or replace view  pv_race_results
as 
select *
 from demo.race_results_python
 where race_year=2010;

-- COMMAND ----------

show tables

-- COMMAND ----------

