# Databricks notebook source
dbutils.widgets.text("p_file_date", "2022-12-31")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../formula1_includes/common_functions

# COMMAND ----------

# MAGIC %run ../formula1_includes/configurations

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")\
    .select("race_year")\
    .distinct()\
    .collect()

# we have a list of row objects, but we need the list of race years
race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)



# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
.filter(col("race_year"). isin(race_year_list))

# checking if the exisiting data contains same race_years of the one from retrieving. If so, only those will be included in the dataframe.

# COMMAND ----------

from pyspark.sql.functions import sum, count, when

# COMMAND ----------

constructor_standings_df = race_results_df \
                      .groupBy("race_year", "team")\
                      .agg(sum("points").alias("total_points"), count(when(race_results_df["position"] == 1, True)).alias("wins"))\
          


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------


merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')


# COMMAND ----------

from delta import DeltaTable

# Re-arrange or process the input DataFrame
final_df = re_arrange_partition_column(final_df, 'race_year')

# Determine the path for the partitioned data in Delta Lake format
delta_partition_path = f"{presentation_folder_path}/constructor_standings"

# Check if the Delta table exists at the path
if DeltaTable.isDeltaTable(spark, delta_partition_path):
    
    # Load the existing Delta table
    existing_data_delta = DeltaTable.forPath(spark, delta_partition_path)

    # converting delta table to dataframe, to perform operations.
    existing_data_delta = existing_data_delta.toDF()

    new_data_df = final_df

    # Find the common 'race_id' values between existing and new data
    common_race_ids = existing_data_delta.select('race_year').intersect(new_data_df.select('race_year'))

    # Check if there are any common 'race_id' values
    if common_race_ids.count() > 0:
        # Delete the data in the existing Delta table for the common 'race_id' values
        existing_data_delta = existing_data_delta.join(common_race_ids, 'race_year', 'left_anti')

     # Union the existing data and the deduplicated new data
        combined_data_df = existing_data_delta.unionByName(new_data_df)

    # Write the deduplicated new data to the existing Delta table
        combined_data_df.write.mode("overwrite").format("delta").save(delta_partition_path)

else:
    # If the Delta table doesn't exist, create it and write data while partitioning by 'race_id'
    final_df.write.mode("overwrite").partitionBy('race_year').format("delta").save(delta_partition_path)


# COMMAND ----------



results_after_df = spark.read.format("delta").load(f"{presentation_folder_path}/constructor_standings")
display(results_after_df)


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM f1_presentation.constructor_standings
# MAGIC GROUP BY race_year