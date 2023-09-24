# Databricks notebook source
# MAGIC %md
# MAGIC ... Trying to access other notebook of another folder to access folder paths (configurations) and ingestion_time function (common_function), inorder to use the parameters in this notebook
# MAGIC

# COMMAND ----------

# MAGIC %run ../formula1_includes/configurations
# MAGIC

# COMMAND ----------

# MAGIC %run ../formula1_includes/common_functions
# MAGIC
# MAGIC

# COMMAND ----------

# Creating a widget text and passing the value into the data. The parameter passed is the race date, so that the incremental load consists of no confusion. 

dbutils.widgets.text("p_file_date", "2023-03-01")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, FloatType

# COMMAND ----------

# redefining schema of the file in order to get it right without using inferschema (it makes spark read the whole data and if the file is huge, it takes a lot of time to compute).
# structtype is a row and struct field is a column.

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False), #nullable = False
                                StructField("raceId", IntegerType(), False),
                                StructField("driverId", IntegerType(), False),
                                StructField("constructorId", IntegerType(), False),
                                StructField("number", IntegerType(), True),
                                StructField("grid", IntegerType(), False),
                                StructField("position", IntegerType(), True),
                                StructField("positionText", StringType(), False),
                                StructField("positionOrder", IntegerType(), False),
                                StructField("points", IntegerType(), False),
                                StructField("laps", IntegerType(), False),
                                StructField("time", StringType(), True),
                                StructField("milliseconds", IntegerType(), True),
                                StructField("fastestLap", IntegerType(), True),
                                StructField("rank", IntegerType(), True),
                                 StructField("fastestLapTime", StringType(), True),
                                StructField("fastestLapSpeed", FloatType(), True),
                                StructField("statusId", IntegerType(), False)])

# COMMAND ----------

# Read the csv file into a dataframe using spark dataframe reader API
# using option with header as true, because the header will be identified and returned in the dataframe. Without it, it will return a dataframe with header as the first row of the dataframe.
# using re-defined schema within the reading part of the file

results_df = spark.read.option("header",True).schema(results_schema).csv(f"{raw_folder_path}/{v_file_date}/results.csv")
display(results_df)

# COMMAND ----------

results_df.columns


# COMMAND ----------

results_df = results_df.dropDuplicates(['resultId',
 'raceId',
 'driverId',
 'constructorId',
 'number',
 'grid',
 'position',
 'positionText',
 'positionOrder',
 'points',
 'laps',
 'time',
 'milliseconds',
 'fastestLap',
 'rank',
 'fastestLapTime',
 'fastestLapSpeed',
 'statusId'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Columns - rename and add audit column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_results_df = results_df.withColumnRenamed('raceId', 'race_id')\
                                .withColumnRenamed('resultId', 'result_id')\
                                .withColumnRenamed('driverId', 'driver_id')\
                                .withColumnRenamed('constructorId', 'constructor_id')\
                                .withColumnRenamed('positionText', 'position_text')\
                                .withColumnRenamed('positionOrder', 'position_order')\
                                .withColumnRenamed('fastestLap', 'fastest_lap')\
                                .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
                                .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))\
                                .drop("statusId")



# select - selecting column
# col(<column-old-name>).alias(<new-name>) - rename column
# withColumn(<new-column>, value to ingest) - adding a new column
# drop - dropping two columns
# concat - combining columns
# to_timestamp - converting string into timestamp

# COMMAND ----------

renamed_results_df = add_ingestion_time(renamed_results_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_results_df)

# COMMAND ----------

renamed_results_df.schema.names

# COMMAND ----------

# MAGIC %md
# MAGIC ##### De Duplicate the dataframe
# MAGIC As we got more than 1 field for - driver_id + race_id. One driver cannot participate multiple times in one race. Hence the duplicates. 

# COMMAND ----------

result_deduped_df = renamed_results_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write this file into data lake storage in the form of a parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1 - add incremental data

# COMMAND ----------

# # When we rerun the code with incremental load again, the data will be added again, and so we need to remove the partition for race_id and make sure that the race_id values are re entered as fresh data.
# # Also the command works only if the table exists, so the first run might fail, hence using spark catalog to include the request to work only if the table exists. 

# for race_id_list in renamed_results_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION(race_id = {race_id_list.race_id})")

# COMMAND ----------

# renamed_results_df.write.mode("append").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# # mode(overwrite) - helps in rewriting the file everytime we update it. 
# # stored the file in processed folder of the data lake, with the name circuits.
# #partitionby - splits file into multiple files depending on the year values.
# # appending the incremental load, instead of using overwrite, as it is not a full load. 

# ###display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic") 

# # Overwrite mode is dynamic, that is, the files will not be completely processed if similar files occur. The new files will be processed dynamically

# COMMAND ----------

# renamed_results_df = renamed_results_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_time", "race_id")
# # changing schema by bringing race_id to the last column.

# COMMAND ----------

# if (spark._jsparkSession.catalog().tableExists("f1_processed.results")): 
#     renamed_results_df.write.mode("overwrite").insertInto("f1_processed.results")
# else:
#     renamed_results_df.write.mode("overwrite").partitionBy("race_id").format("parquet").saveAsTable("f1_processed.results")

# # What's happening is that, if the table exists, the extra folders will be inserted into the table without partition, hence the extra folders will only be processed. 
# # Else, a new table will be created.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Instead of adding the same method in the rest of the files, we made a common function out of it and paste it in common_function in formula1_includes folder.

# COMMAND ----------

#overwrite_partition(renamed_results_df, 'f1_processed', 'results', 'race_id')
# the above is used when dealing with data lake, the below merge is for delta lake storage.

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(result_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Sending the values into storage account : incremental load method applied to database is now also updated to storage account.

# COMMAND ----------

# def file_exists(path):
# 	try:
# 		dbutils.fs.ls(path)
# 		return True
# 	except Exception as e:
# 		if 'java.io.FileNotFoundException' in str(e):
# 			return False

# # Re-arrange or process the input DataFrame
# renamed_results_df = re_arrange_partition_column(renamed_results_df, 'race_id')

# # Set partition overwrite mode to dynamic
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# # Determine the path for the partitioned data
# partition_path = f"{processed_folder_path}/results"

# # Check if the partition_path exists
# if file_exists(partition_path):
#     # Load the existing data from the partition
#     existing_data_df = spark.read.parquet(partition_path)

#     # Deduplicate the new data based on the 'race_id' column
#     new_data_df = renamed_results_df#.dropDuplicates(['race_id'])

#     # Find the common 'race_id' values between existing and new data
#     common_race_ids = existing_data_df.select('race_id').intersect(new_data_df.select('race_id'))
    
#     # Check if there are any common 'race_id' values
#     if common_race_ids.count() > 0:
#         # Drop the data in the existing file for the common 'race_id' values
#         existing_data_df = existing_data_df.join(common_race_ids, 'race_id', 'left_anti')

#     # Union the existing data and the deduplicated new data
#     combined_data_df = existing_data_df.unionByName(new_data_df)

#     # Write the combined data to the partition
#     combined_data_df.write.mode("overwrite").format("delta").save(partition_path)


# else:
#     # If the partition doesn't exist, create it and write data while partitioning by 'race_id'
#     renamed_results_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(partition_path)



# COMMAND ----------

from delta import DeltaTable

# Re-arrange or process the input DataFrame
result_deduped_df = re_arrange_partition_column(result_deduped_df, 'race_id')

# Determine the path for the partitioned data in Delta Lake format
delta_partition_path = f"{processed_folder_path}/results"

# Check if the Delta table exists at the path
if DeltaTable.isDeltaTable(spark, delta_partition_path):
    
    # Load the existing Delta table
    existing_data_delta = DeltaTable.forPath(spark, delta_partition_path)

    # converting delta table to dataframe, to perform operations.
    existing_data_delta = existing_data_delta.toDF()

    new_data_df = result_deduped_df

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
    result_deduped_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(delta_partition_path)


# COMMAND ----------

results_after_df = spark.read.format("delta").load(f"{processed_folder_path}/results")
display(results_after_df.groupBy('file_date').count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY file_date
# MAGIC ORDER BY file_date DESC
# MAGIC
# MAGIC -- SELECT *
# MAGIC -- FROM f1_processed.results
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS f1_processed.results