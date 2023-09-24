-- Databricks notebook source
-- MAGIC %python
-- MAGIC
-- MAGIC # for creating headline for dashboard after visualization
-- MAGIC html = """<h1 style = " color : Black; text-align : center; fomt-family : Ariel "> Report On Dominant Formula 1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

USE f1_presentation

-- COMMAND ----------

/* creating rank for the order */
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM calculated_race_results
GROUP BY team_name
HAVING total_races > 50 /*for aggregated data, where clause won't work.*/ /* 100 - coz a team has 2 members and 50 will be too less.*/
ORDER BY avg_points DESC

-- COMMAND ----------

/* creating visuals to see the domination of drivers over the years with average points */

SELECT race_year,
       team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

/* creating visuals to see the domination of drivers over the years with average points */

SELECT race_year,
       team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <= 10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC