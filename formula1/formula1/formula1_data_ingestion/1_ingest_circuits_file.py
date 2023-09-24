# Databricks notebook source
# MAGIC %md
# MAGIC #### Dealing with ingesting a csv file.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ... Trying to access other notebook of another folder to access folder paths (configurations) and ingestion_time function (common_function), inorder to use the parameters in this notebook
# MAGIC

# COMMAND ----------

# MAGIC %run "../formula1_includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run ../formula1_includes/common_functions

# COMMAND ----------


# Creating a widget text and passing the value into the data. The parameter passed is the race date, so that the incremental load consists of no confusion. 

dbutils.widgets.text("p_file_date", "2015-12-31")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# redefining schema of the file in order to get it right without using inferschema (it makes spark read the whole data and if the file is huge, it takes a lot of time to compute).
# structtype is a row and struct field is a column.

df_schema = StructType(fields = [StructField("circuitId", IntegerType(), False), #nullable = False
                                StructField("circuitRef", StringType(), True),
                                StructField("name", StringType(), True),
                                StructField("location", StringType(), True),
                                StructField("country", StringType(), True),
                                StructField("lat", DoubleType(), True),
                                StructField("lng", DoubleType(), True),
                                StructField("alt", IntegerType(), True),
                                StructField("url", StringType(), True)])

# COMMAND ----------

# Read the csv file into a dataframe using spark dataframe reader API
# using option with header as true, because the header will be identified and returned in the dataframe. Without it, it will return a dataframe with header as the first row of the dataframe.
# using re-defined schema within the reading part of the file

circuit_df = spark.read.option("header",True).schema(df_schema).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
display(circuit_df)

# COMMAND ----------

# let's check if the schemas are defined as required
circuit_df.printSchema()

# COMMAND ----------

circuit_df.columns


# COMMAND ----------

circuit_df = circuit_df.dropDuplicates(['circuitId',
 'circuitRef',
 'name',
 'location',
 'country',
 'lat',
 'lng',
 'alt',
 'url'])

# COMMAND ----------

# MAGIC %md
# MAGIC ####### Columns - select, rename and add audit column

# COMMAND ----------

from pyspark.sql.functions import col, lit
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Creating a widget text and passing the value into the data. The value for the parameter used in here, will be passed from notebook - 0_ingest_all_files.

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")
 

# COMMAND ----------

renamed_cir_df = circuit_df.select(col("circuitId").alias("circuit_id") \
                                    ,col("circuitRef").alias("circuit_ref") \
                                    ,"name", "location", "country"\
                                    ,col("lat").alias("latitude"),col("lng").alias("longitude")\
                                    ,col("alt").alias("altitude"))\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))
                        #           .withColumn("ingestion_time", current_timestamp())\

                                    
                                    


# select - selecting column
# col(<column-old-name>).alias(<new-name>) - rename column
# withColumn(<new-column>, value to ingest) - adding a new column

# COMMAND ----------

renamed_cir_df = add_ingestion_time(renamed_cir_df) 

#this is how we can access other notebook's (common_functions) parameters

# COMMAND ----------

display(renamed_cir_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### write this file into data lake storage in the form of a parquet

# COMMAND ----------

renamed_cir_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")
renamed_cir_df.write.mode("overwrite").format("delta").save(f"{processed_folder_path}/circuits")


# mode(overwrite) - helps in rewriting the file everytime we update it. 
# stored the file in processed folder of the data lake, with the name circuits.

###display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

dbutils.notebook.exit("success")