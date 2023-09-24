-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Create new database for presentation files and save them as tables.

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION 'abfss://presentation@dbprojectformula1.dfs.core.windows.net';


-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results = spark.read.parquet("abfss://presentation@dbprojectformula1.dfs.core.windows.net/race_results")
-- MAGIC driver_standings = spark.read.parquet("abfss://presentation@dbprojectformula1.dfs.core.windows.net/driver_standings")
-- MAGIC constructor_standings = spark.read.parquet("abfss://presentation@dbprojectformula1.dfs.core.windows.net/constructor_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results.write.format("parquet").saveAsTable("f1_presentation.race_results")
-- MAGIC
-- MAGIC driver_standings.write.format("parquet").saveAsTable("f1_presentation.driver_standings")
-- MAGIC
-- MAGIC constructor_standings.write.format("parquet").saveAsTable("f1_presentation.constructor_standings")
-- MAGIC
-- MAGIC