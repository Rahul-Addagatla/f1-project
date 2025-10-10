-- Databricks notebook source
-- Finding dominant drivers between the years 2001 and 2010
SELECT driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC;

-- COMMAND ----------

--Finding dominant teams between the years 2001 and 2010
SELECT team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING COUNT(1) >= 100
ORDER BY avg_points DESC;

-- COMMAND ----------

-- Finding dominant driver of all time
SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

-- Finding dominant team od all time
SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC
