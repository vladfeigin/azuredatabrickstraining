// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This is a WordCount example with the following:
// MAGIC * Event Hubs as a Structured Streaming Source
// MAGIC * Stateful operation (groupBy) to calculate running counts
// MAGIC 
// MAGIC #### Requirements
// MAGIC 
// MAGIC * Databricks version 3.5 or 4.0 and beyond. 

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://eventhubdemons303474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NehTzvWKVB7456H0J0OAPmE4mI7GuBX+Etalx5KhgUQ=")
  .setEventHubName("eh1-303474")
  .build
val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

// split lines by whitespaces and explode the array as rows of 'word'
val df = eventhubs.select(explode(split($"body".cast("string"), "\\s+")).as("word"))
  .groupBy($"word")
  .count

// COMMAND ----------

// follow the word counts as it updates
display(df.select($"word", $"count"))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Try it yourself
// MAGIC 
// MAGIC The Event Hubs Source also includes the ingestion timestamp of records. Try counting the words by the ingestion time window as well.
