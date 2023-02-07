# Databricks notebook source
# MAGIC %md # Join the Flight and Weather datasets

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

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "sp=racwdlmeo&st=2023-02-04T09:29:31Z&se=2023-03-04T17:29:31Z&spr=https&sv=2021-06-08&sr=c&sig=CfujDbdCE2LuJpPEnaq9ooexPK3zN5kf4gbEX8vMlWY%3D")

# COMMAND ----------

# MAGIC %md With both datasets ready, we want to join them together so that we can associate historical flight delays with the weather data at departure time.

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     d.OriginAirportCode,
# MAGIC     d.Month, d.DayofMonth, d.CRSDepHour, d.DayOfWeek,
# MAGIC     d.Carrier, d.DestAirportCode, d.DepDel15, w.WindSpeed,
# MAGIC     w.SeaLevelPressure, w.HourlyPrecip
# MAGIC FROM flight_delay_silver d
# MAGIC INNER JOIN 
# MAGIC     flight_weather_silver w ON
# MAGIC                  d.OriginAirportCode = w.AirportCode AND
# MAGIC                  d.Month = w.Month AND
# MAGIC                  d.DayofMonth = w.Day AND
# MAGIC                  d.CRSDepHour = w.Hour

# COMMAND ----------

--
dfFlightDelaysWithWeather = _sqldf

# COMMAND ----------

# MAGIC %md Now let's take a look at the combined data.

# COMMAND ----------

display(flight_delays_with_weather)

# COMMAND ----------

# MAGIC %md Write the combined dataset to a new persistent global table.

# COMMAND ----------

#dfFlightDelaysWithWeather.write.mode("overwrite").save("/mnt/sparkcontainer/Gold/flight_delays_with_weather")

dfFlightDelaysWithWeather.write.mode("overwrite").option("header","true").\
csv(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/gold/FlightDelayWithWeather/")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flight_delays_with_weather;
# MAGIC CREATE TABLE flight_delays_with_weather
# MAGIC USING CSV LOCATION "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/gold/FlightDelayWithWeather/"

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(*) from flight_delays_with_weather
