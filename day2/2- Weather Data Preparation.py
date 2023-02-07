# Databricks notebook source
# MAGIC %md # Prepare the weather data

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_name", "")

# COMMAND ----------

storage_account = getArgument("storage_account")
container_name = getArgument("container_name")
print (storage_account)
print (container_name)

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
# MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

# COMMAND ----------

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
import math
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md To begin, take a look at the `flight_with_weather_bronze` data that was imported to get a sense of the data we will be working with.

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md Next, count the number of records so we know how many rows we are working with.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md Observe that this data set has 406,516 rows and 29 columns. For this model, we are going to focus on predicting delays using WindSpeed (in MPH), SeaLevelPressure (in inches of Hg), and HourlyPrecip (in inches). We will focus on preparing the data for those features.

# COMMAND ----------

# MAGIC %md Let's start out by taking a look at the **WindSpeed** column. You may scroll through the values in the table above, but reviewing just the distinct values will be faster.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct WindSpeed from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md Try clicking on the **WindSpeed** column header to sort the list by ascending and then by descending order. Observe that the values are all numbers, with the exception of some having `null` values and a string value of `M` for Missing. We will need to ensure that we remove any missing values and convert WindSpeed to its proper type as a numeric feature.

# COMMAND ----------

# MAGIC %md Next, let's take a look at the **SeaLevelPressure** column in the same way, by listing its distinct values.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SeaLevelPressure from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md Like you did before, click on the **SeaLevelPressure** column header to sort the values in ascending and then descending order. Observe that many of the features are of a numeric value (e.g., 29.96, 30.01, etc.), but some contain the string value of M for Missing. We will need to replace this value of "M" with a suitable numeric value so that we can convert this feature to be a numeric feature.

# COMMAND ----------

# MAGIC %md Finally, let's observe the **HourlyPrecip** feature by selecting its distinct values.

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct HourlyPrecip from flight_with_weather_bronze

# COMMAND ----------

# MAGIC %md Click on the column header to sort the list and ascending and then descending order. Observe that this column contains mostly numeric values, but also `null` values and values with `T` (for Trace amount of rain). We need to replace T with a suitable numeric value and convert this to a numeric feature.

# COMMAND ----------

# MAGIC %md ## Clean up weather data

# COMMAND ----------

# MAGIC %md To preform our data cleanup, we will execute a Python script, in which we will perform the following tasks:
# MAGIC 
# MAGIC * WindSpeed: Replace missing values with 0.0, and “M” values with 0.005
# MAGIC * HourlyPrecip: Replace missing values with 0.0, and “T” values with 0.005
# MAGIC * SeaLevelPressure: Replace “M” values with 29.92 (the average pressure)
# MAGIC * Round “Time” column down to the nearest hour, and add value to a new column named “Hour”
# MAGIC * Eliminate unneeded columns from the dataset

# COMMAND ----------

# MAGIC %md Let's begin by creating a new DataFrame from the table. While we're at it, we'll pare down the number of columns to just the ones we need (AirportCode, Month, Day, Time, WindSpeed, SeaLevelPressure, HourlyPrecip).

# COMMAND ----------

dfWeather = spark.sql("select AirportCode, Month, Day, Time, WindSpeed, SeaLevelPressure, HourlyPrecip from flight_with_weather_bronze")

# COMMAND ----------

dfWeather.show()

# COMMAND ----------

# MAGIC %md Review the schema of the dfWeather DataFrame

# COMMAND ----------

dfWeather.printSchema()

# COMMAND ----------


# Round Time down to the next hour, since that is the hour for which we want to use flight data. Then, add the rounded Time to a new column named "Hour", and append that column to the dfWeather DataFrame.
df = dfWeather.withColumn('Hour', F.floor(dfWeather['Time']/100))

# Replace any missing HourlyPrecip and WindSpeed values with 0.0
df = df.fillna('0.0', subset=['HourlyPrecip', 'WindSpeed'])

# Replace any WindSpeed values of "M" with 0.005
df = df.replace('M', '0.005', 'WindSpeed')

# Replace any SeaLevelPressure values of "M" with 29.92 (the average pressure)
df = df.replace('M', '29.92', 'SeaLevelPressure')

# Replace any HourlyPrecip values of "T" (trace) with 0.005
df = df.replace('T', '0.005', 'HourlyPrecip')

# Be sure to convert WindSpeed, SeaLevelPressure, and HourlyPrecip columns to float
# Define a new DataFrame that includes just the columns being used by the model, including the new Hour feature
dfWeather_Clean = df.select('AirportCode', 'Month', 'Day', 'Hour', df['WindSpeed'].cast('float'), df['SeaLevelPressure'].cast('float'), df['HourlyPrecip'].cast('float'))


# COMMAND ----------

# MAGIC %md Now let's take a look at the new `dfWeather_Clean` DataFrame.

# COMMAND ----------

display(dfWeather_Clean)

# COMMAND ----------

# MAGIC %md Observe that the new DataFrame only has 7 columns. Also, the WindSpeed, SeaLevelPressure, and HourlyPrecip fields are all numeric and contain no missing values. To ensure they are indeed numeric, we can take a look at the DataFrame's schema.

# COMMAND ----------

dfWeather_Clean.printSchema()

# COMMAND ----------

# MAGIC %md Now let's persist the cleaned weather data to a persistent global table.

# COMMAND ----------

dfWeather_Clean.write.mode("overwrite").save(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/silver/FlightWeather")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flight_weather_silver;
# MAGIC 
# MAGIC CREATE TABLE flight_weather_silver
# MAGIC USING DELTA LOCATION "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/silver/FlightWeather"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended flight_weather_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flight_weather_silver

# COMMAND ----------


