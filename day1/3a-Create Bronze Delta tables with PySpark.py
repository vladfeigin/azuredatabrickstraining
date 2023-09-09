# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Bronze tables with pySpark
# MAGIC Similary as we created Bronze tables with SQL in the previous exercise in this notebook we create delta tables with pySpark and SQL
# MAGIC
# MAGIC Note that table name in this example is the same as file name (without .csv extension). Enter correct table into `table_name` widget in the notebook top.
# MAGIC
# MAGIC For every new table enter the table name in the widgets in the top of this notebok and run the same notebook without changing anything

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("file_name", "")

# COMMAND ----------

storage_account = getArgument("storage_account")
container_name = getArgument("container_name")
file_name = getArgument("file_name") 
table_name = file_name.split(".")[0] + "_bronze"

print (f"storage account: {storage_account}")
print (f"container name: {container_name}")
print (f"file name: {file_name}")
print (f"table name: {table_name}")

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwlmeo&st=2023-09-07T14:17:14Z&se=2023-11-30T23:17:14Z&spr=https&sv=2022-11-02&sr=c&sig=jyWEvg%2FzLmK9J%2BOxIp%2B8QSCKYpVmNPfKNcNIo68Rh6E%3D")

# COMMAND ----------

# Load CSV file
df = spark.read.csv(f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/{file_name}', header=True, inferSchema="true")

# COMMAND ----------

display(df)

# COMMAND ----------

#table location
bronze_table_location = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/{table_name}/'
print (f"bronze_table_location: {bronze_table_location}")
#write data in delta format to this location
df.write.format("delta").mode("overwrite").save(bronze_table_location)

# COMMAND ----------

display(dbutils.fs.ls(bronze_table_location)) 

# COMMAND ----------

display(dbutils.fs.ls(bronze_table_location+"_delta_log/"))

# COMMAND ----------

#check the written data validity
df_bronze = spark.read.format("delta").load(bronze_table_location, header=True)

# COMMAND ----------

display(df_bronze)


# COMMAND ----------

#total number of rows should be 2719418. Do you have the same? 
df_bronze.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partitioning data (Optional)
# MAGIC This section is optional. You can skip it to: 
# MAGIC **Create external table pointing to the files we just copied**
# MAGIC
# MAGIC Often there is a need to partition data for better query performance.
# MAGIC
# MAGIC Most common way to partition data is by record date or ingestion date but also based on the most common filters you use in queries.

# COMMAND ----------

bronze_table_location_partitioned = f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze_partitioned/{table_name}'
print (bronze_table_location_partitioned)

# COMMAND ----------

df.write.format("delta").partitionBy("Year","Month","DayofMonth").mode("overwrite").save(bronze_table_location_partitioned)

# COMMAND ----------

display(dbutils.fs.ls(bronze_table_location_partitioned)) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create external table pointing to the files we just copied

# COMMAND ----------

# MAGIC %md
# MAGIC We created Delta files, now it's time to create External table pointing to those files.

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights_demo

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table_name} 
USING DELTA 
LOCATION '{bronze_table_location}' 
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from airportcodelocationlookupclean_bronze

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# _sqldf is a built in Spark DataFrame created automatically when you work with SQL. It allows easily to mix pySpark and SQL codes in the same notebook
_sqldf.count()

# COMMAND ----------

# MAGIC %sql show tables 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Exercise 
# MAGIC
# MAGIC Create a new **SQL** notebook for creating Delta Tables in Bronze. Create all 3 external tables in Bronze layer.
# MAGIC
# MAGIC Hint: 
# MAGIC
# MAGIC       1. Use Temporary views which reads data from original CSV files with "inferSchema" = "true"
# MAGIC
# MAGIC       2. Use CTAS to create External tables
# MAGIC       
# MAGIC When you create External table, use ***path*** option(see example in the second notebook _"2-Data exploration with SQL"_ ):
# MAGIC
# MAGIC ***options(
# MAGIC   path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightWithWeather/"
# MAGIC )***   to indicate location of the files in datalake.
# MAGIC
# MAGIC
# MAGIC Create partitioned table (select any table you want for this exercise)
# MAGIC
# MAGIC Hint: 
# MAGIC
# MAGIC In CTAS when you create query, use ***partitioned by (Year,Month,DayofMonth)*** 
# MAGIC
# MAGIC
# MAGIC for example:
# MAGIC  
# MAGIC _create table my_part_table_name
# MAGIC using delta
# MAGIC options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/')
# MAGIC partitioned by (Year,Month,DayofMonth)
# MAGIC as
# MAGIC select * from my_temp_view_
# MAGIC
# MAGIC Run query by using partition fields (in WHERE clause)
