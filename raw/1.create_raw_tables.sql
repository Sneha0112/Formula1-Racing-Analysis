-- Databricks notebook source
create database if not exists f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Circuits Table

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(circuitId int,
circuitRef string,
name string,
location string,
country string,
lat double,
lng double,
alt int,
url string)
using csv
options (path "/mnt/formula1deltalakeproject/raw/circuits.csv",header true)

-- COMMAND ----------

select * from f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Races Table

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(raceId int,
year int,
round int,
circuitId int,
name string,
date date,
time string,
url string)
using csv
options(path "/mnt/formula1deltalakeproject/raw/races.csv",header true) 

-- COMMAND ----------

select * from f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create constructors table 
-- MAGIC 1. single line json
-- MAGIC 2. simple structure

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
constructorId int,
constructorRef string,
name string,
nationality string,
url string)
using json
options(path "/mnt/formula1deltalakeproject/raw/constructors.json")

-- COMMAND ----------

select * from f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create driver table
-- MAGIC 1. Single line json
-- MAGIC 2. Complex structure

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
driverId int,
driverRef string,
number int,
code string,
name struct<forename:string, surname:string>,
dob date,
nationality string,
url string)
using json
options(path "/mnt/formula1deltalakeproject/raw/drivers.json")

-- COMMAND ----------

select * from f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create result table
-- MAGIC 1. single line json
-- MAGIC 2. simple structure 

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
resultId int,
raceId int,
driverId int,
constructorId int,
number int,grid int,
position int,
positionText string,
positionOrder int,
points int ,
laps int,
time string,
milliseconds int,
fastestLap int,
rank int,
fastestLapTime string,
fastestLapSpeed float,
statusId string)
using json
options(path "/mnt/formula1deltalakeproject/raw/results.json")

-- COMMAND ----------

select * from f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create pit stops table
-- MAGIC 1. Multi Line JSON
-- MAGIC 2. Simple structure

-- COMMAND ----------

drop table if exists f1_raw.pit_stops;
create table if not exists f1_raw.pit_stops(
driverId int,
duration string,
lap int,
milliseconds int,
raceId int,
stop int,
time string)
using json
options(path "/mnt/formula1deltalakeproject/raw/pit_stops.json",multiLine true)

-- COMMAND ----------

select * from f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Lap Times Table
-- MAGIC 1. CSV File
-- MAGIC 2. Multiple Files

-- COMMAND ----------

drop table if exists f1_raw.lap_times;
create table if not exists f1_raw.lap_times(
raceId int,
driverId int,
lap int,
position int,
time string,
milliseconds int
)
using csv
options(path "/mnt/formula1deltalakeproject/raw/lap_times")

-- COMMAND ----------

select count(1) from f1_raw.lap_times;

-- COMMAND ----------

select * from f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Qualifying table
-- MAGIC 1. JSON file
-- MAGIC 2. Multiline JSON
-- MAGIC 3. Multiple files

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
constructorId int,
driverId int,
number int,
position int,
q1 string,
q2 string,
q3 string,
qualifyId int,
raceId int
)
using json
options(path "/mnt/formula1deltalakeproject/raw/qualifying",multiLine true)

-- COMMAND ----------

select * from f1_raw.qualifying;

-- COMMAND ----------

desc extended f1_raw.qualifying;

-- COMMAND ----------

