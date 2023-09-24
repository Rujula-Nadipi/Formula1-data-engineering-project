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

# COMMAND ----------

# Creating a widget text and passing the value into the data. The parameter passed is the race date, so that the incremental load consists of no confusion. 

dbutils.widgets.text("p_file_date", "2023-03-01")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

# redefining schema of the file in order to get it right without using inferschema (it makes spark read the whole data and if the file is huge, it takes a lot of time to compute).
# structtype is a row and struct field is a column.

df_schema = StructType(fields = [StructField("raceId", IntegerType(), False), #nullable = False
                                StructField("year", IntegerType(), True),
                                StructField("round", IntegerType(), True),
                                StructField("circuitId", IntegerType(), True),
                                StructField("name", StringType(), True),
                                StructField("date", StringType(), True),
                                StructField("time", StringType(), True),
                                StructField("url", StringType(), True)])

# COMMAND ----------

# Read the csv file into a dataframe using spark dataframe reader API
# using option with header as true, because the header will be identified and returned in the dataframe. Without it, it will return a dataframe with header as the first row of the dataframe.
# using re-defined schema within the reading part of the file

race_df = spark.read.option("header",True).schema(df_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")
display(race_df)

# COMMAND ----------

# let's check if the schemas are defined as required
race_df.printSchema()

# COMMAND ----------

race_df.columns


# COMMAND ----------

race_df = race_df.dropDuplicates(['raceId', 'year', 'round', 'circuitId', 'name', 'date', 'time', 'url'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Columns - select, rename, club and add audit column

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import current_timestamp, to_timestamp

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_race_df = race_df.select( col("raceId").alias("race_id"), col("year").alias("race_year"), "round"\
                                , col("circuitId").alias("circuit_id"), "name", "date", "time")\
                                . withColumn("race_imestamp"\
                                , to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss'))\
                                . drop("time","date")\
                                . withColumn("data_source", lit(v_data_source))\
                                . withColumn("file_date", lit(v_file_date))

              #                  . withColumn("ingestion_Time", current_timestamp())



# select - selecting column
# col(<column-old-name>).alias(<new-name>) - rename column
# withColumn(<new-column>, value to ingest) - adding a new column
# drop - dropping two columns
# concat - combining columns
# to_timestamp - converting string into timestamp

# COMMAND ----------

renamed_race_df = add_ingestion_time(renamed_race_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write this file into data lake storage in the form of a parquet

# COMMAND ----------

renamed_race_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races1")
renamed_race_df.write.mode("overwrite").partitionBy("race_year").format("delta").save(f"{processed_folder_path}/races")


# mode(overwrite) - helps in rewriting the file everytime we update it. 
# stored the file in processed folder of the data lake, with the name circuits.
#partitionby - splits file into multiple files depending on the year values.

### display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

dbutils.notebook.exit("success")