# Databricks notebook source
# MAGIC %md
# MAGIC ##### Create Bronze tables with pySpark
# MAGIC Similary as we created Bronze tables with SQL in the previous exercise in this notebook we create delta tables with pySpark and SQL
# MAGIC 
# MAGIC Note that table name in this example is the same as file name (without .csv extension). Enter correct table into `table_name` widget in the notebook top.
# MAGIC 
# MAGIC For every new table enter the table name in the widgets in the top of this notebok and run the same notebook without changing anything

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")
dbutils.widgets.text("table_name", "")

# COMMAND ----------

storage_account = getArgument("storage_account")
container_name = getArgument("container_name")
table_name = getArgument("table_name")
file_name = table_name + ".csv"
print (storage_account)
print (container_name)
print (file_name)
print (table_name)

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

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
# MAGIC ##### Partitioning data
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

# This way we pass parameters to the SQL, which is NOT a widget  
# First define Spark parameter in Python cell 
spark.conf.set("tables.location", bronze_table_location)

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- Second do SET of this parameter in %sql set. See below how we use `table.location` parameter 
# MAGIC SET table_location = ${tables.location}

# COMMAND ----------

# MAGIC %md
# MAGIC We created Delta files and also partitioned it, not it's time to create External table pointing to those files.

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ${table_name};
# MAGIC CREATE TABLE ${table_name}
# MAGIC USING DELTA LOCATION '${tables.location}'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${table_name} limit 10

# COMMAND ----------

# MAGIC %sql describe extended ${table_name}

# COMMAND ----------

# MAGIC %sql select count(1) from ${table_name}

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Exercise1
# MAGIC Create External Table in `flights` data base but based on the partitioned files (by Year, Month, DayofMonths)
# MAGIC 
# MAGIC Note that partitioned Delta files are located here in the variable `bronze_table_location_partitioned`
# MAGIC 
# MAGIC First define Spark conf parameters as we did before in Python cell.
# MAGIC 
# MAGIC Second in %sql SET part_table_location = ${tables.location}
# MAGIC 
# MAGIC Create table in `flight` DB.
# MAGIC 
# MAGIC Test table by running count of rows. The resulst should be ***2719418*** rows.
# MAGIC 
# MAGIC Finally run SQL query with WHERE by year, month, dayofmonth and count how many flights where in some specific day.

# COMMAND ----------

Exercise2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Exercise 2
# MAGIC Create a new SQL notebook for creating Delta Tables in Bronze by using solely SQL. 
# MAGIC 
# MAGIC Hint: 
# MAGIC 
# MAGIC       1. Use Temporary views which reads data from original CSV files with "inferSchema" = "true"
# MAGIC 
# MAGIC       2. Use CTAS to create External tables
# MAGIC       
# MAGIC When you create External table, use:
# MAGIC 
# MAGIC ***options(
# MAGIC   path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightWithWeather/"
# MAGIC )*** 
# MAGIC 
# MAGIC To indicate location of the files in datalake
# MAGIC 
# MAGIC See first notebook with examples.
# MAGIC 
# MAGIC 
# MAGIC Create Partitioned table (select anyone you want for this exercise)
# MAGIC 
# MAGIC Hint: 
# MAGIC 
# MAGIC Use **** partitioned by (Year,Month,DayofMonth) *** 
# MAGIC 
# MAGIC  in CTAS when you create query for example:
# MAGIC  
# MAGIC _create table my_part_table_name
# MAGIC using delta
# MAGIC options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/')
# MAGIC partitioned by (Year,Month,DayofMonth)
# MAGIC as
# MAGIC select * from my_temp_view_
# MAGIC 
# MAGIC Run query by using partition fields.

# COMMAND ----------


