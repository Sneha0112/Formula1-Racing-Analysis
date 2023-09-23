-- Databricks notebook source
select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

select * from f1_presentation.calculated_race_results;

-- COMMAND ----------

select * 
  from f1_presentation.calculated_race_results;

-- COMMAND ----------

SELECT driver_name ,
    COUNT(1) AS total_races, 
    SUM(calculated_points) AS total_points,
    AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

select driver_name ,
COUNT(1) as total_races, 
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2011 and 2020
group by driver_name
having count(1) >= 50
order by avg_points desc;

-- COMMAND ----------

select driver_name ,
COUNT(1) as total_races, 
SUM(calculated_points) as total_points,
AVG(calculated_points) as avg_points
from f1_presentation.calculated_race_results
where race_year between 2001 and 2020
group by driver_name
having count(1) >= 50
order by avg_points desc;

-- COMMAND ----------

