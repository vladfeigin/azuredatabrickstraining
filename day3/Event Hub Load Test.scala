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


spark.conf.set(s"fs.azure.account.auth.type.$storage_account.dfs.core.windows.net", "SAS")
spark.conf.set(s"fs.azure.sas.token.provider.type.$storage_account.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(s"fs.azure.sas.fixed.token.$storage_account.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

// COMMAND ----------

val abfssPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/"
println(abfssPath)

// COMMAND ----------

// MAGIC %sql 
// MAGIC use flights

// COMMAND ----------

// MAGIC %sql
// MAGIC restore table flight_delay_bronze version as of 1

// COMMAND ----------

// MAGIC %sql 
// MAGIC select count(*) from flight_delay_bronze

// COMMAND ----------

import java.time.Duration
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://eventhubdemons303474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NehTzvWKVB7456H0J0OAPmE4mI7GuBX+Etalx5KhgUQ=")
  .setEventHubName("flight-weather-20-partitions-test")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  //.setStartingPosition(EventPosition.fromEndOfStream)
    .setReceiverTimeout(Duration.ofSeconds(300))
    .setOperationTimeout(Duration.ofSeconds(500))  
    .setMaxEventsPerTrigger(10000) //previous value was 1000
    .setStartingPosition(EventPosition.fromStartOfStream) 

// COMMAND ----------

// Read from delta lake table

import org.apache.spark.sql.functions.{to_json, struct, col}

val tabledf = spark
  .readStream
  .option("ignoreChanges", "true")
  .option("maxBytesPerTrigger", 8096)
  .format("delta")
  .table("flight_delay_bronze")
  .select("*")
  .select(to_json(struct(col("*"))).alias("body")).drop("*")

// COMMAND ----------


val checkpointPath = s"abfss://$container_name@$storage_account.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze/checkpoint_flight_delay_bronze"


// COMMAND ----------

//write from delta table to EH 
import org.apache.spark.sql.streaming.Trigger

val query =
  tabledf
    .writeStream
    .format("eventhubs")
    .queryName("write from delta table to EH")   
    .outputMode("update") 
    .options(eventHubsConf.toMap)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.ProcessingTime("0 seconds"))
    .start()

// COMMAND ----------

//read from event hub
val ehreaddf = spark
  .readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()
//val ehdatadf = df.select($"body" cast "string")

// COMMAND ----------

val outputPath = s"abfss://$containerName@$storageAccount.dfs.core.windows.net/FlightsDelays/stream/flightdelay/output_eh_2_dl"

val checkpointPath = s"abfss://$containerName@$storageAccount.dfs.core.windows.net/FlightsDelays/stream/flightdelay/checkpoint_eh_2_dl/"


// COMMAND ----------

ehreaddf
  .writeStream
  .format("json")
  .queryName("write from eh to datalake")
  .outputMode("append")
  .option("path", outputPath)
  .option("checkpointLocation", checkpointPath)
  .start()


// COMMAND ----------

// MAGIC %md 
// MAGIC ##### generate additional stream of changes

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view DL_records_view
// MAGIC as select * from flight_delay_bronze where 
// MAGIC --Carrier = 'DL'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into flight_delay_bronze 
// MAGIC select * from DL_records_view

// COMMAND ----------

// MAGIC %md ##### check number of delivered rows

// COMMAND ----------

// MAGIC %sql select count(*) from flight_delay_bronze

// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists flight_delay_bronze_streamed_eh;
// MAGIC create table flight_delay_bronze_streamed_eh 
// MAGIC using json options(
// MAGIC 'path' "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightdelay/output_eh_2_dl"
// MAGIC )

// COMMAND ----------

// MAGIC %sql refresh table flight_delay_bronze_streamed_eh

// COMMAND ----------

// MAGIC %sql select count(*) from flight_delay_bronze_streamed_eh 

// COMMAND ----------


