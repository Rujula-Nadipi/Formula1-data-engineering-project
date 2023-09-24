# Databricks notebook source
# MAGIC %md
# MAGIC ### setting up access to storage account using SAS token(SAS are in the storage account)
# MAGIC 1. set up spark configuration  - fs.azure.account.key.
# MAGIC 2. list documents from demo container. (*as it is in the azure data storage account)
# MAGIC 3. read circuits.csv file from demo container.

# COMMAND ----------

# to create spark configuration - we need three lines:
# 1. two parts - authorizing SAS - (a) fs.azure.account.key (b) endpoint to the storage account
# 2. providers with a type of SAS  - fixed SAS token provider.
# 3. token 

spark.conf.set("fs.azure.account.auth.type.dbprojectformula1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dbprojectformula1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dbprojectformula1.dfs.core.windows.net", "sp=rl&st=2023-08-21T21:47:22Z&se=2023-08-22T05:47:22Z&sv=2022-11-02&sr=c&sig=PzgPndcGZnCAREtUL1N%2BEUA7jtOk%2F2Dq9Y4UVDJOb7E%3D")

 # be careful while uploading in github as the key will be exposed.

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