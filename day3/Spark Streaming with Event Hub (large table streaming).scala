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
// MAGIC show tables

// COMMAND ----------

// MAGIC %sql
// MAGIC restore table flight_delay_bronze version as of 0

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from flight_delay_bronze

// COMMAND ----------

//Set connection string to event hub

import java.time.Duration
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
//ConnectionStringBuilder(s"Endpoint=sb://eventhubdemons303474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=$EVENT_HUB_SECRET")
val connectionString=
ConnectionStringBuilder(s"Endpoint=sb://vlad-etoro-tests.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=$EVENT_HUB_SECRET")
  .setEventHubName("etoro-test-15-part")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  //.setStartingPosition(EventPosition.fromEndOfStream)
    .setReceiverTimeout(Duration.ofSeconds(300))
    .setOperationTimeout(Duration.ofSeconds(500))  
    .setMaxEventsPerTrigger(10000)
    .setStartingPosition(EventPosition.fromStartOfStream) 

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Read from delta table and write to event hub

// COMMAND ----------

// Read from delta lake table

import org.apache.spark.sql.functions.{to_json, struct, col}

val tabledf = spark
  .readStream
  .option("ignoreChanges", "true")
  .option("maxBytesPerTrigger", 12*1024*1024)
  .format("delta")
  .table("flight_delay_bronze")
  .select("*")
  .select(to_json(struct(col("*"))).alias("body")).drop("*")

// COMMAND ----------

//define checkpoint for stream
val checkpointPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/checkpoint_flight_delay_bronze"


// COMMAND ----------

// .trigger(Trigger.ProcessingTime("0 seconds"))
//.repartition(10)
//.trigger(Trigger.ProcessingTime("10 seconds"))
//write from delta table to EH 
import org.apache.spark.sql.streaming.Trigger

val query =
  tabledf.repartition(15)
    .writeStream
    .format("eventhubs")
    .queryName("write from delta table to EH")   
    .outputMode("update") 
    .options(eventHubsConf.toMap)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.AvailableNow())
    .start()

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Read from event hub and write to delta table

// COMMAND ----------

//read from event hub
val ehreaddf = spark
  .readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
//val ehdatadf = df.select($"body" cast "string")

// COMMAND ----------

val outputPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightdelay/output_eh_2_dl"
val checkpointPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightdelay/checkpoint_eh_2_dl/"


// COMMAND ----------

ehreaddf
  .writeStream
  .format("delta")
  .queryName("write from eh to datalake")
  .outputMode("append")
  .option("path", outputPath)
  .option("checkpointLocation", checkpointPath)
  .start()


// COMMAND ----------

// MAGIC %md ##### check number of delivered rows

// COMMAND ----------

// MAGIC %sql select count(*) from flight_with_weather_bronze

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists flight_with_weather_bronze_streamed_eh;
// MAGIC create table flight_with_weather_bronze_streamed_eh
// MAGIC using json options(
// MAGIC 'path' "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightwithweather/output_eh_2_dl"
// MAGIC )

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
// MAGIC select * from flight_with_weather_bronze

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view DL_records_view
// MAGIC as select * from flight_delay_bronze 

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into flight_delay_bronze 
// MAGIC select * from DL_records_view

// COMMAND ----------


