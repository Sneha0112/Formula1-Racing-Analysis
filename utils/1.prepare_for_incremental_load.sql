-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

create database if not exists f1_processed
location "/mnt/formula1deltalakeproject/processed";

-- COMMAND ----------

drop database if exists f1_presentation CASCADE;

-- COMMAND ----------

create database if not exists f1_presentation
location "/mnt/formula1deltalakeproject/presentation";