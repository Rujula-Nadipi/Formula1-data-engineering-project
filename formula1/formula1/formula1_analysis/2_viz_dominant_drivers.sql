-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # for creating headline for dashboard after visualization
-- MAGIC html = """<h1 style = " color : Black; text-align : center; fomt-family : Ariel "> Report On Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

USE f1_presentation

-- COMMAND ----------

/* creating rank for the order */
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM calculated_race_results
GROUP BY driver_name
HAVING total_races > 50 /*for aggregated data, where clause won't work.*/ /* 100 - coz a team has 2 members and 50 will be too less.*/
ORDER BY avg_points DESC

-- COMMAND ----------

/* creating visuals to see the domination of drivers over the years with average points */

SELECT race_year,
       driver_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

/* creating visuals to see the domination of drivers over the years with average points */

SELECT race_year,
       driver_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC