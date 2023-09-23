-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/formula1deltalakeproject/processed"

-- COMMAND ----------

desc database f1_processed;

-- COMMAND ----------

