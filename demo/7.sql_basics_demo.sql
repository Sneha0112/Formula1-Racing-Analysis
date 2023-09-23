-- Databricks notebook source
show databases;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from driver_standings
limit 10;

-- COMMAND ----------

desc driver_standings;

-- COMMAND ----------

select * from driver_standings
where driver_nationality='Italian' and total_points >=10
order by team asc,total_points desc;

-- COMMAND ----------

