# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Access ADLS Using SAS token
# MAGIC 
# MAGIC Note that the recommended way to access ADLS from Databricks is by using AAD Service Principal and the backed by Azure Key Vault Databricks Secret Scope.
# MAGIC 
# MAGIC Here for simplicity we use SAS token.
# MAGIC 
# MAGIC Note: Replace the storage account and coontainer with your names  

# COMMAND ----------

storage_account='asastoremcw303474'
container_name = 'labs-303474'
data_folder_name = 'FlightsDelays'

# COMMAND ----------

# Set up Spark configuration for SAS token in order to access Azure Datalake
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ABFS protocol
# MAGIC 
# MAGIC ABFS protocol (Azure Blob File System) - Azure Blob storage driver for Hadoop required by Spark.
# MAGIC ABFS is part of Apache Hadoop and is included in many of the commercial distributions of Hadoop. It's a recommended protocol today to use when working with ADLS v2
# MAGIC 
# MAGIC The objects in ADLS are represented as URIs with the following URI schema:
# MAGIC 
# MAGIC _abfs[s]://container_name@account_name.dfs.core.windows.net/<path>/<path>/<file_name>_
# MAGIC   
# MAGIC If you add an **_'s'_** at the end (abfss) then the ABFS Hadoop client driver will ALWAYS use Transport Layer Security (TLS) irrespective of the authentication method chosen.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dbutils
# MAGIC 
# MAGIC Databricks utility tool
# MAGIC 
# MAGIC You can do many opertations with dbutils, for more details see [dbutils](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
# MAGIC 
# MAGIC In this example we list the content of ADLS folder _FlightsDelays_ in ADLS container:

# COMMAND ----------

display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Initial data exploration

# COMMAND ----------

# MAGIC %md
# MAGIC Load file to Spark DataFrame (pySpark)

# COMMAND ----------

df = spark.read.csv(f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv', header=True)

# COMMAND ----------

# Useful display method 
display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC Run Spark operations of DataFrame we've just created

# COMMAND ----------

# Print file schema in dataframe 
df.printSchema()

# COMMAND ----------

# Infer schema with inferSchema="True" Spark option
df = spark.read.csv(f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv', header=True, inferSchema="true")

# COMMAND ----------

# let's check the updated schema in the new DataFrame....
df.printSchema()

# COMMAND ----------

# count number of records in the file
display(df.count())

# COMMAND ----------

# Using filter in DataFrame. Count total number of delays
display(df.filter(df["DepDel15"] == "1").count())

# COMMAND ----------

# Check how many flights per month. Sort output by click on "count" header.
res = df.groupBy("Month").count()
display(res)

# COMMAND ----------

#distribution of values for our target column DepDel15
display(df.groupBy("DepDel15").count())

# COMMAND ----------

# Check how many flights per month
# We can see that July is most busy months 

from pyspark.sql import functions as F
# Group the data by the Month column and count the number of rows in each group ans sort it
res = df.groupBy("Month").agg(F.count("*").alias("Count")).sort(F.desc("Count"))
display(res)

# COMMAND ----------

#Example of Spark SQL. Count how many flights per origin airport
#load dataframe into temporary view
df.createOrReplaceTempView("FLIGHTS_DATA")

# Use Spark SQL to get the number of flights for each origin airport code
most_loaded_airport = spark.sql("""
  SELECT OriginAirportName, Month, COUNT(*) as flights
  FROM FLIGHTS_DATA
  GROUP BY OriginAirportName,Month
  ORDER BY flights DESC
""")

# Show the result
display(most_loaded_airport)

# COMMAND ----------

# count how many delays per Origin airport

from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F

#Calculate number of delays per origin airport
delay_counts = df.groupBy("OriginAirportName").agg({"DepDel15": "sum"}).limit(10)
#Rename autogenerated column to human readable
delay_counts = delay_counts.withColumnRenamed("sum(DepDel15)", "DelayCount")
display(delay_counts.sort(F.desc("DelayCount")))   

# COMMAND ----------

# Count all cases of double delay: in Origin and Destination 
from pyspark.sql import functions as F

# Filter only the rows where DepDel15 is equal to 1 (delayed flights)
delayed_flights = df.filter(df["DepDel15"] == "1")

# Group the data by OriginAirportCode and DestAirportName and count the number of delayed flights
result = delayed_flights.groupBy("OriginAirportName", "DestAirportName").agg(F.count("DepDel15").alias("Delay Count")).sort(F.desc("Delay Count"))

# Show the result
display(result)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Data Quality exploration

# COMMAND ----------

# MAGIC %md Check the number of null values in DepDel15, this colimn states the departure delay of at least      in 15 min.
# MAGIC 
# MAGIC If we want to use this field for delay prediction we should fix those records with null values. 

# COMMAND ----------

df = spark.read.csv(f'abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv', header=True)
display(df)

# COMMAND ----------

#simple check of missing or invalid values with distinct for the target field: DepDel15
display(df.select('DepDel15').distinct())

# COMMAND ----------

#check percentage of missing values for our target field: DepDel15
from pyspark.sql.functions import col

percentage_nulls_in_DepDel15 = df.filter(col("DepDel15").isNull() ).count() / df.count() * 100
print (f"{percentage_nulls_in_DepDel15} % null values in DepDel15 column") 

# COMMAND ----------

#let's do it simpler with dbutils
dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Exercise 
# MAGIC Run the same analysis for FlightWeatherWithAirportCode.csv file.
# MAGIC Start from cell number 6
