// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.widgets.removeAll()
// MAGIC dbutils.widgets.text("storage_account", "")
// MAGIC dbutils.widgets.text("container_name", "")

// COMMAND ----------

// MAGIC %python
// MAGIC storage_account = dbutils.widgets.get("storage_account")
// MAGIC container_name = dbutils.widgets.get("container_name")
// MAGIC print (storage_account)
// MAGIC print (container_name)

// COMMAND ----------

// MAGIC %python
// MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
// MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
// MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

// COMMAND ----------

//display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/"))
val containerName = "labs-303474"
val storageAccount = "asastoremcw303474"
val abfssPath = s"abfss://$containerName@$storageAccount.dfs.core.windows.net/FlightsDelays/"
println(abfssPath)

// COMMAND ----------

// MAGIC %sql 
// MAGIC use flights

// COMMAND ----------

import java.time.Duration
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://eventhubdemons303474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NehTzvWKVB7456H0J0OAPmE4mI7GuBX+Etalx5KhgUQ=")
  .setEventHubName("eh1-303474")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  //.setStartingPosition(EventPosition.fromEndOfStream)
    .setReceiverTimeout(Duration.ofSeconds(300))
     .setMaxEventsPerTrigger(500)

// COMMAND ----------


import org.apache.spark.sql.functions.{to_json, struct, col}

val dfs = spark
  .readStream
  .option("ignoreChanges", "true")
  .option("maxBytesPerTrigger", 1024)
  .format("delta")
  .table("flight_delay_bronze")
  .select("*")
  .select(to_json(struct(col("*"))).alias("body")).drop("*")

// COMMAND ----------


val checkpointPath = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze/checkpoint_scala/"


// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
//.trigger(ProcessingTime("25 seconds"))
val query =
  dfs
    .writeStream
    .format("eventhubs")
    .outputMode("update") 
    .options(eventHubsConf.toMap)
    .option("checkpointLocation", checkpointPath)
    .trigger(Trigger.ProcessingTime("0 seconds"))
    .start()

// COMMAND ----------

// MAGIC %sql
// MAGIC create or replace temp view DL_records_view
// MAGIC as select * from flight_delay_bronze where 
// MAGIC Carrier = 'DL'

// COMMAND ----------

// MAGIC %sql select count(*) from DL_records_view

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into flight_delay_bronze 
// MAGIC select * from DL_records_view

// COMMAND ----------

val df = spark
  .readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

val ehdatadf = df.select($"body" cast "string")
//display(ehdatadf)

// COMMAND ----------

val outputPath = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze/output_eh_2_dl"

val checkpointPath = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze/checkpoint_eh_to_dl/"


// COMMAND ----------

ehdatadf.repartition(1)
  .writeStream
  .format("json")
  .queryName("write from eh to datalake")
  .outputMode("append")
  .option("path", outputPath)
  .option("checkpointLocation", checkpointPath)
  .start()


// COMMAND ----------

// MAGIC %sql 
// MAGIC drop table if exists flight_delay_bronze_streamed_eh;
// MAGIC create table flight_delay_bronze_streamed_eh 
// MAGIC using json options(
// MAGIC 'path' "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/stream/flightdelaybronze/output_eh_2_dl"
// MAGIC )

// COMMAND ----------

// MAGIC %sql select count(*) from flight_delay_bronze

// COMMAND ----------

// MAGIC %sql refresh table flight_delay_bronze_streamed_eh

// COMMAND ----------

// MAGIC %sql select count(*) from flight_delay_bronze_streamed_eh

// COMMAND ----------


