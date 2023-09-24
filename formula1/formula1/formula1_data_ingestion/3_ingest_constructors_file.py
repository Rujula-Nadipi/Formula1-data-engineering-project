# Databricks notebook source
# MAGIC %md
# MAGIC ##### Trying to deal with a different schema, where redefining will not include struct type and struct fields. Let's see how it goes

# COMMAND ----------

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

# redefining schema 
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.option("headers", True).schema(constructors_schema).csv(f"{raw_folder_path}/{v_file_date}/constructors.csv")

display(constructors_df)

# COMMAND ----------

constructors_df.columns


# COMMAND ----------

constructors_df = constructors_df.dropDuplicates(['constructorId', 'constructorRef', 'name', 'nationality', 'url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Columns - select, rename and include audit column

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_con_df = constructors_df.select( col("constructorId").alias("constructor_id")\
                                        , col("constructorRef").alias("constructor_ref")\
                                        , "name", "nationality")\
                                        . withColumn("data_source", lit(v_data_source))\
                                        . withColumn("file_date", lit(v_file_date))
                    #                    . withColumn("Ingestion_Time", current_timestamp())

# COMMAND ----------

renamed_con_df = add_ingestion_time(renamed_con_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_con_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingesting the file into data lake of parquet format

# COMMAND ----------

renamed_con_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
renamed_con_df.write.mode("overwrite").format("delta").save(f"{processed_folder_path}/constructors")

### display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("success")