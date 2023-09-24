# Databricks notebook source
# MAGIC %md
# MAGIC ### cluster scoped authentication using secret scope:
# MAGIC 1. after creating access keys and SAS tokens, we create a key vault in azure (a resource). Then, we connect it with secret scope in databricks, by extending the homepage url of databricks with "/secrets/createScope".
# MAGIC 2. Next, we give access to the cluster by using the key value pair in spark config of the cluster. 
# MAGIC 3. instead of key, we give "{{secrets/<scope-name/key-name(the name for the key given in key vault)>}}

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