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

drivers_schema = StructType(fields = [StructField("driverId", IntegerType(), True), #nullable = True
                                StructField("driverRef", StringType(), True),
                                StructField("number", IntegerType(), False),
                                StructField("code", StringType(), True),
                                StructField("forename", StringType(), True),
                                StructField("surname", StringType(), True),
                                StructField("dob", StringType(), True),
                                StructField("nationality", StringType(), True),
                                StructField("url", StringType(), True)
                                ])

# COMMAND ----------

# Read the csv file into a dataframe using spark dataframe reader API
# using option with header as true, because the header will be identified and returned in the dataframe. Without it, it will return a dataframe with header as the first row of the dataframe.
# using re-defined schema within the reading part of the file

drivers_df = spark.read.option("header",True).schema(drivers_schema).csv(f"{raw_folder_path}/{v_file_date}/drivers.csv")
display(drivers_df)

# COMMAND ----------

# let's check if the schemas are defined as required
drivers_df.printSchema()

# COMMAND ----------

drivers_df.columns


# COMMAND ----------

drivers_df = drivers_df.dropDuplicates(['driverId',
 'driverRef',
 'number',
 'code',
 'forename',
 'surname',
 'dob',
 'nationality',
 'url'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Columns - select, rename, club and add audit column

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_drivers_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("driverRef", "driver_ref")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))\
                                .withColumn("name", concat(col("forename"), lit(" "), col("surname")))\
                                .drop("url")

 #.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
# select - selecting column
# col(<column-old-name>).alias(<new-name>) - rename column
# withColumn(<new-column>, value to ingest) - adding a new column
# drop - dropping two columns
# concat - combining columns
# to_timestamp - converting string into timestamp

# COMMAND ----------

renamed_drivers_df = add_ingestion_time(renamed_drivers_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write this file into data lake storage in the form of a parquet

# COMMAND ----------

renamed_drivers_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")
renamed_drivers_df.write.mode("overwrite").format("delta").save(f"{processed_folder_path}/drivers")

# mode(overwrite) - helps in rewriting the file everytime we update it. 
# stored the file in processed folder of the data lake, with the name circuits.
#partitionby - splits file into multiple files depending on the year values.

### display(spark.read.parquet(f"{processed_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("success")