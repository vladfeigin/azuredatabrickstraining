# Databricks notebook source
# MAGIC %md
# MAGIC ##### Create Bronze tables with pySpark
# MAGIC Similary as we created Bronze tables with SQL in the previous exercise in this notebook we create delta tables with pySpark and SQL
# MAGIC 
# MAGIC Note that table name in this example is the same as file name (without .csv extension)

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("table_name", "")

# COMMAND ----------

storage_account = getArgument("storage_account")
container_name = getArgument("container_name")
table_name = getArgument("table_name")
file_name = table_name + ".csv"
print (storage_account)
print (container_name)
print (file_name)
print (table_name)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

# COMMAND ----------

# Load CSV file
df = spark.read.csv(f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/{file_name}', header=True, inferSchema="true")

# COMMAND ----------

display(df)

# COMMAND ----------

#table location
bronze_table_location = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/{table_name}/'
#write data in delta format to this location
df.write.format("delta").mode("overwrite").save(bronze_table_location)

# COMMAND ----------

display(dbutils.fs.ls(bronze_table_location)) 

# COMMAND ----------

#check the written data validity
df_bronze = spark.read.format("delta").load(bronze_table_location, header=True)

# COMMAND ----------

display(df_bronze)


# COMMAND ----------

#total number of rows should be 2719418 
df_bronze.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Partitioning data
# MAGIC Often there is a need to partition data.
# MAGIC Most common way to partition data is by record date or ingestion date.

# COMMAND ----------

table_name = table_name + "_part" 
bronze_table_location_partitioned = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/{table_name}'
print (bronze_table_location_partitioned)

# COMMAND ----------

df.write.format("delta").partitionBy("Year","Month","DayofMonth").mode("overwrite").save(bronze_table_location_partitioned)

# COMMAND ----------

display(dbutils.fs.ls(bronze_table_location_partitioned)) 

# COMMAND ----------

spark.conf.set("tables.location", bronze_table_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET table_location = ${tables.location}

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${table_name};
# MAGIC CREATE TABLE ${table_name}
# MAGIC USING DELTA LOCATION '${tables.location}'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${table_name} limit 10

# COMMAND ----------

# MAGIC %sql describe extended ${table_name}

# COMMAND ----------

# MAGIC %sql select count(1) from ${table_name}

# COMMAND ----------


