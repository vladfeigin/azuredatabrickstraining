// Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")

// COMMAND ----------


val storage_account = dbutils.widgets.get("storage_account")
val container_name = dbutils.widgets.get("container_name")
println (storage_account)
println (container_name)

// COMMAND ----------

//Initialize access to container 
spark.conf.set(s"fs.azure.account.auth.type.$storage_account.dfs.core.windows.net", "SAS")
spark.conf.set(s"fs.azure.sas.token.provider.type.$storage_account.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(s"fs.azure.sas.fixed.token.$storage_account.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

// COMMAND ----------

//read env variables (in cluster level)
val EVENT_HUB_SECRET = sys.env.get("EVENT_HUB_SECRET_KEY").getOrElse("")

// COMMAND ----------

// MAGIC %sql 
// MAGIC use flights

// COMMAND ----------

// MAGIC %sql
// MAGIC restore table flight_with_weather_bronze version as of 0

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from flight_with_weather_bronze

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Read from delta table and write to other delta table

// COMMAND ----------

// Read from delta lake table
import org.apache.spark.sql.functions.{to_json, struct, col}

val tabledf = spark
  .readStream
  .option("ignoreChanges", "true")
  .option("maxBytesPerTrigger", 5096)
  .format("delta")
  .table("flight_with_weather_bronze")
  .select("*")

// COMMAND ----------

//define checkpoint for stream
val outputPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/output_t1_2_t2"
val checkpointPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/checkpoint_t1_2_t2/"

// COMMAND ----------

//write from delta table to EH 
import org.apache.spark.sql.streaming.Trigger

val query =
  tabledf
    .writeStream
    .format("delta")
    .queryName("write from flight_with_weather_bronze to new delta table")   
    .outputMode("append") 
    .option("path", outputPath)
    .option("checkpointLocation", checkpointPath) 
   // .trigger(Trigger.ProcessingTime("0 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ##### check number of delivered rows

// COMMAND ----------

// MAGIC %sql select count(*) from flight_with_weather_bronze

// COMMAND ----------

spark.conf.set("table.location", outputPath)

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists flight_with_weather_bronze_streamed_eh;
// MAGIC create table flight_with_weather_bronze_streamed_eh
// MAGIC using delta location '${table.location}'

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- clean spark cache
// MAGIC refresh table flight_with_weather_bronze_streamed_eh

// COMMAND ----------

// MAGIC %sql select count(*) from flight_with_weather_bronze_streamed_eh

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### generate additional stream of changes
// MAGIC See how the stream being activated again to read the changes

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from flight_with_weather_bronze_streamed_eh

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view DL_records_view
// MAGIC as select * from flight_with_weather_bronze where AirportCode = 'SJU'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into flight_with_weather_bronze 
// MAGIC select * from DL_records_view

// COMMAND ----------


