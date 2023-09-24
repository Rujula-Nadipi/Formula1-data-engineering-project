# Databricks notebook source
# a common function that is used in all the notebooks, therefore we can access this notebook and utilize it easily, instead of writing the whole function.

from pyspark.sql.functions import current_timestamp

def add_ingestion_time(input_df):
    output_df = input_df.withColumn("ingestion_time", current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column in input_df.schema.names:
        if column != partition_column:
            column_list.append(column)
    column_list.append(partition_column)

    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------



# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): 
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
        
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC 1. The code sets a Spark configuration using spark.conf.set to set the partition overwrite mode to "dynamic". This means that when partitions are being overwritten, Spark will attempt to dynamically optimize the process for better performance.
# MAGIC 2. This code block checks if the table specified by db_name and table_name already exists in the Spark catalog. If the table exists, the output_df DataFrame is written to the table using the "overwrite" mode and the insertInto function. This means that existing data will be replaced entirely with the new data, and the partition structure is retained.
# MAGIC 3. If the specified table does not exist, this block of code is executed. It writes the output_df DataFrame to the specified table name and database name. The data is written in "overwrite" mode, meaning any existing data in the table will be replaced. The data is partitioned by the partition_column, and the storage format used is Parquet. This data is then saved as a table in the Spark catalog.

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):

    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    from delta.tables import DeltaTable 

    if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")): 
        
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
       
        deltaTable.alias("tgt").merge(
        input_df.alias("src"), 
        merge_condition)\
        .whenNotMatchedInsertAll()\
        .whenMatchedUpdateAll()\
        .execute()

        deltaTable = deltaTable.toDF()

        deltaTable.write.mode("overwrite").format("delta").saveAsTable(f"{db_name}.{table_name}")
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

