# Databricks notebook source
# MAGIC %md
# MAGIC ### setting up access to storage account using access keys (access keys are in the storage account)
# MAGIC 1. set up spark configuration  - fs.azure.account.key.
# MAGIC 2. list documents from demo container. (*as it is in the azure data storage account)
# MAGIC 3. read circuits.csv file from demo container.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### but the issue is that using access keys will give access to the whole storage account, instead is we want to provide some restrictions to users for few files and be able to access the ones that are needed, we use shared access signatures token (SAS)

# COMMAND ----------

# to create spark configuration - we need two lines:
# 1. two parts - (a) fs.azure.account.key (b) endpoint to the storage account
# 2. access key from storage account.

spark.conf.set(
    "fs.azure.account.key.dbprojectformula1.dfs.core.windows.net",
    "hA0tDLuW6K2iMQJ2qfR02vP9fF6Fh6pWSAcNicWumWwTZHWYkenaKNRdrswDbqBVvpiBUK66O8lS+ASt9uXMfQ==") # be careful while uploading in github as the key will be exposed.

# COMMAND ----------

# Listing the documents - access storage account.

dbutils.fs.ls("abfss://demo@dbprojectformula1.dfs.core.windows.net")
# abfss - databricks recommends to use azure blob file system storage driver to access the storage account. 
# then container name @ endpoint to storage account. 

# COMMAND ----------

# for a better readable format of displaying the files:

display(dbutils.fs.ls("abfss://demo@dbprojectformula1.dfs.core.windows.net"))

# COMMAND ----------

# now to read the file: using spark dataframe reader API.

spark.read.csv("abfss://demo@dbprojectformula1.dfs.core.windows.net/circuits.csv") #path in the brackets.

# COMMAND ----------

# again for a better readable display format:

display(spark.read.csv("abfss://demo@dbprojectformula1.dfs.core.windows.net/circuits.csv"))