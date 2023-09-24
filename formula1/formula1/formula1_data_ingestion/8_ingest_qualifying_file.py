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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

# redefining schema of the file in order to get it right without using inferschema (it makes spark read the whole data and if the file is huge, it takes a lot of time to compute).
# structtype is a row and struct field is a column.

qualifying_schema = StructType(fields = [StructField("qualifyId", IntegerType(), False),
                                StructField("raceId", IntegerType(), False), #nullable = False
                                StructField("driverId", IntegerType(), False),
                                StructField("constructorId", IntegerType(), False),
                                StructField("number", IntegerType(), False),
                                StructField("position", IntegerType(), True),
                                StructField("q1", StringType(), True),
                                StructField("q2", StringType(), True),
                                StructField("q3", StringType(), True)
                                ])

# COMMAND ----------

# Read the csv file into a dataframe using spark dataframe reader API
# using option with header as true, because the header will be identified and returned in the dataframe. Without it, it will return a dataframe with header as the first row of the dataframe.
# using re-defined schema within the reading part of the file

qualifying_df = spark.read.schema(qualifying_schema).csv(f"{raw_folder_path}/{v_file_date}/qualifying.csv")
display(qualifying_df)

# COMMAND ----------

qualifying_df.count() #counting number of records, to see if the combination of multiple files is achieved.

# COMMAND ----------

qualifying_df.columns


# COMMAND ----------

qualifying_df = qualifying_df.dropDuplicates(['qualifyId',
 'raceId',
 'driverId',
 'constructorId',
 'number',
 'position',
 'q1',
 'q2',
 'q3'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Columns - rename and add audit column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_qualifying_df = qualifying_df.withColumnRenamed('raceId', 'race_id')\
                                     .withColumnRenamed('driverId', 'driver_id')\
                                     .withColumnRenamed('qualifyId', 'qualify_id')\
                                     .withColumnRenamed('constructorId', 'constructor_id')\
                                     .withColumn("data_source", lit(v_data_source))\
                                     .withColumn("file_date", lit(v_file_date))
                             


# select - selecting column
# col(<column-old-name>).alias(<new-name>) - rename column
# withColumn(<new-column>, value to ingest) - adding a new column
# drop - dropping two columns
# concat - combining columns
# to_timestamp - converting string into timestamp

# COMMAND ----------

renamed_qualifying_df = add_ingestion_time(renamed_qualifying_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write this file into data lake storage in the form of a parquet

# COMMAND ----------

# overwrite_partition(renamed_qualifying_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

# def file_exists(path):
#     try:
#         dbutils.fs.ls(path)
#         return True
#     except Exception as e:
#         if 'java.io.FileNotFoundException' in str(e):
#             return False

# # Re-arrange or process the input DataFrame
# renamed_qualifying_df = re_arrange_partition_column(renamed_qualifying_df, 'race_id')

# # Set partition overwrite mode to dynamic
# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# # Determine the path for the partitioned data
# partition_path = f"{processed_folder_path}/qualifying"

# # Check if the partition_path exists
# if file_exists(partition_path):
#     # Load the existing data from the partition
#     existing_data_df = spark.read.parquet(partition_path)

#     # Deduplicate the new data based on the 'race_id' column
#     new_data_df = renamed_qualifying_df#.dropDuplicates(['race_id'])

#     # Find the common 'race_id' values between existing and new data
#     common_race_ids = existing_data_df.select('race_id').intersect(new_data_df.select('race_id'))
    
#     # Check if there are any common 'race_id' values
#     if common_race_ids.count() > 0:
#         # Drop the data in the existing file for the common 'race_id' values
#         existing_data_df = existing_data_df.join(common_race_ids, 'race_id', 'left_anti')

#     # Union the existing data and the deduplicated new data
#     combined_data_df = existing_data_df.unionByName(new_data_df)

#     # Write the combined data to the partition
#     combined_data_df.write.mode("overwrite").parquet(partition_path)

# else:
#     # If the partition doesn't exist, create it and write data while partitioning by 'race_id'
#     renamed_qualifying_df.write.mode("overwrite").partitionBy('race_id').parquet(partition_path)




# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(renamed_qualifying_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------



from delta import DeltaTable

# Re-arrange or process the input DataFrame
renamed_qualifying_df = re_arrange_partition_column(renamed_qualifying_df, 'race_id')

# Determine the path for the partitioned data in Delta Lake format
delta_partition_path = f"{processed_folder_path}/qualifying"

# Check if the Delta table exists at the path
if DeltaTable.isDeltaTable(spark, delta_partition_path):
    
    # Load the existing Delta table
    existing_data_delta = DeltaTable.forPath(spark, delta_partition_path)

    # converting delta table to dataframe, to perform operations.
    existing_data_delta = existing_data_delta.toDF()

    new_data_df = renamed_qualifying_df

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
    renamed_qualifying_df.write.mode("overwrite").partitionBy('race_id').format("delta").save(delta_partition_path)






# COMMAND ----------

results_after_df = spark.read.format("delta").load(f"{processed_folder_path}/qualifying")
display(results_after_df.groupBy('file_date').count())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id

# COMMAND ----------

dbutils.notebook.exit("success")