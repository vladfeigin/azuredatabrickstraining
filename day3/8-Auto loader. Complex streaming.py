# Databricks notebook source
# MAGIC %md
# MAGIC # Incremental Data Ingestion with Auto Loader
# MAGIC 
# MAGIC ![](https://files.training.databricks.com/images/autoloader-detection-modes.png)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC [Directory Listing in Blob Storage](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/directory-listing-mode)
# MAGIC 
# MAGIC Directory listing (default). Auto loader identifies new files by listing the input directory.
# MAGIC 
# MAGIC [Notification in Blob Storage](https://learn.microsoft.com/en-us/azure/databricks/ingestion/auto-loader/file-notification-mode)
# MAGIC 
# MAGIC File notification. Auto Loader automatically set sup 

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
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-03-25T10:44:33Z&se=2023-05-31T18:44:33Z&spr=https&sv=2021-12-02&sr=c&sig=0cKmo1mfJvwXXAaViczNXGSXI60BMsPM5urqr9WBSNQ%3D")

# COMMAND ----------

from pyspark.sql.functions import avg, from_json, window
from pyspark.sql.types import StructType, StructField, StringType

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  #.option("cloudFiles.useNotifications", "true") notification option
                  .option("cloudFiles.schemaLocation", checkpoint_directory)
                  .load(data_source)
                  .select("device_id", "heartrate", "time")
                  .groupBy("device_id")
                  .agg(avg("heartrate")
                  .alias("avg_heartrate"))     
                  .writeStream
                  .outputMode("complete") 
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .option("AvailableNow", "true") 
                  .table(table_name))
    return query

# COMMAND ----------

dbutils.fs.ls( "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/autoloader_source_folder/")

# COMMAND ----------

input_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/autoloader_source_folder/input_batch/"
checkpoint_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/autoloader_source_folder/checkpoint_batch/"
output_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/autoloader_source_folder/output_batch/"

# COMMAND ----------

query = autoload_to_table(data_source = input_path,
                          source_format = "json",
                          table_name = "complex_streaming_target_aggr",
                          checkpoint_directory = checkpoint_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM complex_streaming_target_aggr

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY streaming_target_table_batch

# COMMAND ----------

# MAGIC %sql SELECT * FROM cloud_files_state("abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/autoloader_source_folder/checkpoint_batch/");

# COMMAND ----------


