-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races
FROM calculated_race_results
GROUP BY team_name
ORDER BY total_points DESC

-- COMMAND ----------

/* with extra number of races, the total points differ a whole lot and doesn't makes sense, so we will include average points instead.*/
 SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
GROUP BY team_name
ORDER BY avg_points DESC

-- COMMAND ----------

/* for 2 total races - if avg points is 10 - then the attended 2 races has 10 points and it's not fair to set it this way, so we include number of races clause to be more than 50. */

SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
GROUP BY team_name
HAVING total_races > 50 /*for aggregated data, where clause won't work.*/
ORDER BY avg_points DESC

-- COMMAND ----------

/* checking who dominated between 2001 and 2020 */

SELECT team_name,
       SUM(calculated_points) AS total_points,
       COUNT(1) AS total_races,
       AVG(calculated_points) AS avg_points
FROM calculated_race_results
WHERE race_year BETWEEN 2001 and 2020
GROUP BY team_name
HAVING total_races > 100 /*for aggregated data, where clause won't work.*/ /* 100 - coz a team has 2 members and 50 will be too less.*/
ORDER BY avg_points DESC