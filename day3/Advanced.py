# Databricks notebook source
# MAGIC %python
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.text("storage_account", "")
# MAGIC dbutils.widgets.text("container_name", "")

# COMMAND ----------

# MAGIC %python
# MAGIC storage_account = getArgument("storage_account")
# MAGIC container_name = getArgument("container_name")
# MAGIC print (storage_account)
# MAGIC print (container_name)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
# MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/"))

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql restore table flight_delay_bronze version as of 0

# COMMAND ----------

# MAGIC %sql select * from flight_delay_bronze;

# COMMAND ----------

_sqldf.rdd.getNumPartitions()

# COMMAND ----------

dfs = spark.readStream \
.option("ignoreChanges", "true") \
.option("maxBytesPerTrigger", 1024) \
.format("delta") \
.table("flight_delay_bronze") \
.select("*") 
#.select(F.to_json(F.struct("*")).alias("body")).drop('*')

# COMMAND ----------

stream_path=f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze"
print (stream_path)
checkpoint_path = stream_path + "/checkpoint/"
print (checkpoint_path)

# COMMAND ----------


query = dfs.repartition(1).\
writeStream.format("json").\
queryName("test1-default-num-part").\
outputMode("append").\
option("path",stream_path).\
option("checkpointLocation", checkpoint_path).\
start()



# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists flight_delay_bronze_streamed;
# MAGIC create table flight_delay_bronze_streamed 
# MAGIC using json options(
# MAGIC 'path' "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze"
# MAGIC )

# COMMAND ----------

# MAGIC %sql REFRESH TABLE  flight_delay_bronze_streamed;

# COMMAND ----------

# MAGIC %sql select * from flight_delay_bronze_streamed

# COMMAND ----------

display(_sqldf.count())

# COMMAND ----------

_sqldf.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view DL_records_view
# MAGIC as select * from flight_delay_bronze where 
# MAGIC Carrier = 'DL'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into flight_delay_bronze 
# MAGIC select * from DL_records_view

# COMMAND ----------



# COMMAND ----------

import datetime
receiverTimeoutDuration = datetime.time(0,3,20).strftime("PT%HH%MM%SS")
print(receiverTimeoutDuration)

# COMMAND ----------


