# Databricks notebook source
# MAGIC %md
# MAGIC ##### This notebook demonstrate simple Spark Streaming capabilities

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")

# COMMAND ----------

storage_account = dbutils.widgets.get("storage_account")
container_name = dbutils.widgets.get("container_name")
print (storage_account)
print (container_name)

# COMMAND ----------

#Initialize access to container 
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-03-21T06:47:36Z&se=2023-06-04T13:47:36Z&spr=https&sv=2021-12-02&sr=c&sig=ioUnTbdgyKcGvCEUWOW875R32Vi8BinW%2BA8SasK7Nlo%3D")

# COMMAND ----------

# MAGIC %sql 
# MAGIC use flight_demo

# COMMAND ----------

# MAGIC %sql show tables

# COMMAND ----------

# MAGIC %md
# MAGIC Stream *flight_with_weather_bronze table* to other table. You can make a data changes directly on the stream. 

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- check the number of rows in the source table
# MAGIC select count(*) from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Read from delta table and write to other delta table

# COMMAND ----------

# Read from delta lake table
# In order to limit input rate you can use : maxFilesPerTrigger (how many new files to be considered in every micro batch, default is 1000). maxBytesPerTrigger (how much data will be processed in each micro-batch)
#from pyspark.sql.functions import to_json, struct, col

tabledf = spark \
    .readStream \
    .option("ignoreChanges", "true") \
    .option("maxBytesPerTrigger", 5096) \
    .format("delta") \
    .table("flight_with_weather_bronze") \
    .select("*")

# COMMAND ----------

#define checkpoint for stream
outputPath = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/output"
checkpointPath = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/checkpoint"

# COMMAND ----------

#write from delta table to new delta table
#another options are:
#.trigger(processingTime = '10 seconds') \
#trigger(once=True) will run the stream as a single batch at once
#trigger(availableNow=True) will find optimal batch sizes for you

query = tabledf \
    .writeStream \
    .format("delta") \
    .queryName("write from flight_with_weather_bronze to new delta table") \
    .outputMode("append") \
    .option("path", outputPath) \
    .option("checkpointLocation", checkpointPath) \
    .start()

# COMMAND ----------

# MAGIC %md ##### check number of delivered rows

# COMMAND ----------

# MAGIC %sql select count(*) from flight_with_weather_bronze

# COMMAND ----------

spark.conf.set("table.location", outputPath)

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists flight_with_weather_bronze_streamed_eh;
# MAGIC create table flight_with_weather_bronze_streamed_eh
# MAGIC using delta location '${table.location}'

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- clean spark cache
# MAGIC refresh table flight_with_weather_bronze_streamed_eh

# COMMAND ----------

# MAGIC %sql select count(*) from flight_with_weather_bronze_streamed_eh

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from flight_with_weather_bronze_streamed_eh limit 10

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### generate additional stream of changes
# MAGIC See how the stream being activated again to read the changes

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view DL_records_view
# MAGIC as select * from flight_with_weather_bronze 
# MAGIC where AirportCode = 'SJU'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into flight_with_weather_bronze 
# MAGIC select * from DL_records_view
