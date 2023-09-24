# Databricks notebook source
# MAGIC %md
# MAGIC 1. With dbutils.notebbok.run, we are trying to run a notebook that contains parameters (dbutils.widgets.text), and we are trying to pass the values for these parameters. We provide notebook name, number of seconds to run and - parameter with value in dictionary format.
# MAGIC 2. Then, inorder to know if it ran successfully or failed, we use dbutils.notebook.exit("<message>") in the executable notebook, such that, that message will be passed into this v_success and be printed successfully.

# COMMAND ----------

v_success = dbutils.notebook.run("1_ingest_circuits_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("2_ingest_races_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("3_ingest_constructors_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("4_ingest_drivers_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("5_ingest_results_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("6_ingest_pitstops_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("7_ingest_laptimes_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success

# COMMAND ----------

v_success = dbutils.notebook.run("8_ingest_qualifying_file", 0, {"p_data_source":"Ergast API", "p_file_date":"2015-12-31"})
v_success