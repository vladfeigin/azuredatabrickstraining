// Databricks notebook source
// MAGIC %md ## Set up Connection to Azure Event Hubs

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val connectionString = ConnectionStringBuilder("Endpoint=sb://eventhubdemons303474.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=NehTzvWKVB7456H0J0OAPmE4mI7GuBX+Etalx5KhgUQ=")
  .setEventHubName("eh1-303474")
  .build

val eventHubsConf = EventHubsConf(connectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

var streamingInputDF = 
  spark.readStream
    .format("eventhubs")
    .options(eventHubsConf.toMap)
    .load()

// COMMAND ----------

// MAGIC %md ## streamingInputDF.printSchema
// MAGIC 
// MAGIC   root <br><pre>
// MAGIC    </t>|-- body: binary (nullable = true) <br>
// MAGIC    </t>|-- offset: string (nullable = false) <br>
// MAGIC    </t>|-- sequenceNumber: long (nullable = false) <br>
// MAGIC    </t>|-- enqueuedTime: timestamp (nullable = false) <br>
// MAGIC    </t>|-- publisher: string (nullable = true) <br>
// MAGIC    </t>|-- partitionKey: string (nullable = true) <br>

// COMMAND ----------

streamingInputDF.printSchema()

// COMMAND ----------

// MAGIC %md ## Sample Event Payload
// MAGIC The `body` column is provided as a `binary`. After applying the `cast("string")` operation, a sample could look like:
// MAGIC <pre>
// MAGIC {
// MAGIC </t>"city": "<CITY>", 
// MAGIC </t>"country": "United States", 
// MAGIC </t>"countryCode": "US", 
// MAGIC </t>"isp": "<ISP>", 
// MAGIC </t>"lat": 0.00, "lon": 0.00, 
// MAGIC </t>"query": "<IP>", 
// MAGIC </t>"region": "CA", 
// MAGIC </t>"regionName": "California", 
// MAGIC </t>"status": "success", 
// MAGIC </t>"hittime": "2017-02-08T17:37:55-05:00", 
// MAGIC </t>"zip": "38917" 
// MAGIC }

// COMMAND ----------

// MAGIC %md ## GroupBy, Count

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body").cast("string"), "$.zip").alias("zip"))
    .groupBy($"zip") 
    .count()

// COMMAND ----------

display(streamingSelectDF)

// COMMAND ----------

// MAGIC %md ## Window

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body").cast("string"), "$.zip").alias("zip"), get_json_object(($"body").cast("string"), "$.hittime").alias("hittime"))
   .groupBy($"zip", window($"hittime".cast("timestamp"), "10 minute", "5 minute", "2 minute"))
   .count()


// COMMAND ----------

// MAGIC %md ## Memory Output

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("memory")        
    .queryName("sample")     
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## Console Output

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("console")        
    .outputMode("complete") 
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## File Output with Partitions

// COMMAND ----------

import org.apache.spark.sql.functions._

var streamingSelectDF = 
  streamingInputDF
   .select(get_json_object(($"body").cast("string"), "$.zip").alias("zip"),    get_json_object(($"body").cast("string"), "$.hittime").alias("hittime"), date_format(get_json_object(($"body").cast("string"), "$.hittime"), "dd.MM.yyyy").alias("day"))
    .groupBy($"zip") 
    .count()
    .as[(String, String)]

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val query =
  streamingSelectDF
    .writeStream
    .format("parquet")
    .option("path", "/mnt/sample/test-data")
    .option("checkpointLocation", "/mnt/sample/check")
    .partitionBy("zip", "day")
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ##### Create Table

// COMMAND ----------

// MAGIC %sql CREATE EXTERNAL TABLE  test_par
// MAGIC     (hittime string)
// MAGIC     PARTITIONED BY (zip string, day string)
// MAGIC     STORED AS PARQUET
// MAGIC     LOCATION '/mnt/sample/test-data'

// COMMAND ----------

// MAGIC %md ## JDBC Sink

// COMMAND ----------

import java.sql._

class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String)] {
      val driver = "com.mysql.jdbc.Driver"
      var connection:Connection = _
      var statement:Statement = _
      
    def open(partitionId: Long,version: Long): Boolean = {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, user, pwd)
        statement = connection.createStatement
        true
      }

      def process(value: (String, String)): Unit = {
        statement.executeUpdate("INSERT INTO zip_test " + 
                "VALUES (" + value._1 + "," + value._2 + ")")
      }

      def close(errorOrNull: Throwable): Unit = {
        connection.close
      }
   }


// COMMAND ----------

val url="jdbc:mysql://<mysqlserver>:3306/test"
val user ="user"
val pwd = "pwd"

val writer = new JDBCSink(url,user, pwd)
val query =
  streamingSelectDF
    .writeStream
    .foreach(writer)
    .outputMode("update")
    .trigger(ProcessingTime("25 seconds"))
    .start()
    

// COMMAND ----------

// MAGIC %md ## EventHubs Sink

// COMMAND ----------

// The connection string for the Event Hub you will WRTIE to. 
val connectionString = "{EVENT HUB CONNECTION STRING}"    
val eventHubsConfWrite = EventHubsConf(connectionString)

val query =
  streamingSelectDF
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("25 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md ## EventHubs Sink - Write test data with Rate Source

// COMMAND ----------

import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

// The connection string for the Event Hub you will WRTIE to. 
val connString = "{EVENT HUB CONNECTION STRING}"    
val eventHubsConfWrite = EventHubsConf(connString)

val source = 
  spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)
    .load()
    .withColumnRenamed("value", "body")
    .select($"body" cast "string")

val query = 
  source
    .writeStream
    .format("eventhubs")
    .outputMode("update")
    .options(eventHubsConfWrite.toMap)
    .trigger(ProcessingTime("25 seconds"))
    .option("checkpointLocation", "/checkpoint/")
    .start()
