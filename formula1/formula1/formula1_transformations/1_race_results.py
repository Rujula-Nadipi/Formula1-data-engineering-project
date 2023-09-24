# Databricks notebook source
# MAGIC %md
# MAGIC #### Requirements:
# MAGIC 1. race_year, race_name, race_date - races
# MAGIC 2. circuit_location - circuits
# MAGIC 3. driver_name, dirver_number, driver_nationality - drivers
# MAGIC 4. team - constructors
# MAGIC 5. grid, fastest_lap, race_time, points - results.
# MAGIC 6. created_date - current_timestamp - common_functions notebook.

# COMMAND ----------

# Creating a widget text and passing the value into the data. The parameter passed is the race date, so that the incremental load consists of no confusion. 

dbutils.widgets.text("p_file_date", "2022-12-31")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run ../formula1_includes/configurations

# COMMAND ----------

# MAGIC %run ../formula1_includes/common_functions

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")
display(races_df)

# COMMAND ----------

# for the first requirment, we need to change names of columns 
renamed_races_df = races_df.withColumnRenamed("name", "race_name")\
                           .withColumnRenamed("race_imestamp", "race_date")\
                          
display(renamed_races_df)

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")
display(circuits_df)

# COMMAND ----------

# for the second requirment, we need to change names
renamed_circuits_df = circuits_df.withColumnRenamed("location", "circuit_location")
                         
display(renamed_circuits_df)

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")
    
display(drivers_df)

# COMMAND ----------

# for the 3rd requirment, we need to change names of columns 
renamed_drivers_df = drivers_df.withColumnRenamed("name", "driver_name")\
                           .withColumnRenamed("number", "driver_number")\
                           .withColumnRenamed("nationality", "driver_nationality")
display(renamed_drivers_df)

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")
  
display(constructors_df)

# COMMAND ----------

# for the 4th requirment, we need to change names of columns
renamed_constructors_df = constructors_df.withColumnRenamed("name", "team")
                         
display(renamed_constructors_df)

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'")

display(results_df)

# COMMAND ----------

# for the 5th requirment, we need to change names of columns
renamed_results_df = results_df.withColumnRenamed("time", "race_time")\
                               .withColumnRenamed("race_id", "results_race_id")\
                               .withColumnRenamed("file_date", "results_file_date")
                         
display(renamed_results_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### According to relational diagram, we need to slowly base connections in a way that requirements are given.
# MAGIC 1. circuits + races = circuit_id
# MAGIC 2. races + results = race_id
# MAGIC 3. results + drivers = driver_id
# MAGIC 4. results + constructors = constructor_id

# COMMAND ----------

# 1
races_circuits_df = renamed_circuits_df.join(renamed_races_df, renamed_circuits_df.circuit_id == renamed_races_df.circuit_id, "inner")\
                                       .select("race_id", "race_year", "race_name", "race_date", "circuit_location")
display(races_circuits_df)

# COMMAND ----------

display(renamed_races_df.select('race_id').distinct())
display(renamed_results_df.select('results_race_id').distinct())

# COMMAND ----------

# 2
races_circuits_results_df = races_circuits_df.join(renamed_results_df, races_df.race_id == renamed_results_df.results_race_id, "inner").select("results_race_id","constructor_id", "driver_id", "race_year", "race_name", "race_date","circuit_location", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date")
display(races_circuits_results_df)

# COMMAND ----------

# 3
races_circuits_results_drivers_df = races_circuits_results_df.join(renamed_drivers_df \
                                    , renamed_results_df.driver_id == renamed_drivers_df.driver_id, "inner")\
                                    . select("results_race_id","constructor_id", "race_year", "race_name", "race_date", "circuit_location", "grid", "fastest_lap", "race_time", "points", "driver_name", "driver_number", "driver_nationality", "position", "results_file_date")
#display(races_circuits_results_drivers_df)

# COMMAND ----------

# 4
final_df = races_circuits_results_drivers_df.join( renamed_constructors_df \
                                    , renamed_results_df.constructor_id == renamed_constructors_df.constructor_id, "inner")\
                                    . select("results_race_id", "race_year", "race_name", "race_date", "circuit_location", "grid", "fastest_lap", "race_time", "points", "driver_name", "driver_number", "driver_nationality", "team", "position", "results_file_date" )\
                                    .withColumnRenamed( "results_file_date" , "file_date")\
                                    .withColumnRenamed( "results_race_id" , "race_id")
#display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### lastly adding ingestion time

# COMMAND ----------

final_df = add_ingestion_time(final_df)
#display(final_df)

# COMMAND ----------


merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

from delta import DeltaTable

# Re-arrange or process the input DataFrame
final_df = re_arrange_partition_column(final_df, 'race_id')

# Determine the path for the partitioned data in Delta Lake format
delta_partition_path = f"{presentation_folder_path}/race_results"

# Check if the Delta table exists at the path
if DeltaTable.isDeltaTable(spark, delta_partition_path):
    
    # Load the existing Delta table
    existing_data_delta = DeltaTable.forPath(spark, delta_partition_path)

    # converting delta table to dataframe, to perform operations.
    existing_data_delta = existing_data_delta.toDF()

    new_data_df = final_df

    # Find the common 'race_id' values between existing and new data
    common_race_ids = existing_data_delta.select('race_id').intersect(new_data_df.select('race_id'))

    # Check if there are any common 'race_id' values
    if common_race_ids.count() > 0:
        # Delete the data in the existing Delta table for the common 'race_id' values
        existing_data_delta = existing_data_delta.join(common_race_ids, 'race_id', 'left_anti')

     # Union the existing data and the deduplicated new data
        combined_data_df = existing_data_delta.unionByName(new_data_df)

    # Write the deduplicated new data to the existing Delta table
        combined_data_df.write.mode("overwrite").format("delta").save(delta_partition_path)

else:
    # If the Delta table doesn't exist, create it and write data while partitioning by 'race_id'
    final_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(delta_partition_path)


# COMMAND ----------

results_after_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")
display(results_after_df.groupBy('race_id').count())


# COMMAND ----------

# %sql
# SELECT race_id, COUNT(1)
#  FROM f1_presentation.race_results
#  GROUP BY race_id 
#  ORDER BY race_id DESC